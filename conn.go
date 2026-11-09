package ipc

import (
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/wzshiming/ipc/internal/control"
	"github.com/wzshiming/ipc/internal/ring"
	"github.com/wzshiming/ipc/internal/shm"
)

// addr is the address of a listener or connection: its socket path.
type addr string

func (addr) Network() string  { return "ipc" }
func (a addr) String() string { return string(a) }

// Conn is one shared-memory connection made of Lanes() independent lanes.
type Conn struct {
	link      *control.Link
	seg       *shm.Segment
	lanes     []*Lane
	ringSize  int
	maxRecord int
	spin      time.Duration
	addr      addr
	sendDir   uint8

	closeOnce  sync.Once
	done       chan struct{}
	err        error // written once before done is closed
	readerDone chan struct{}

	live     atomic.Int32 // lane halves that may still touch the segment
	unmapped atomic.Bool  // test hook: the segment has been released
}

// newConn wires lanes onto the rings of seg and starts the control reader.
// On error the caller still owns link and seg.
func newConn(link *control.Link, seg *shm.Segment, lanes, ringSize int, spin time.Duration, path string, listener bool) (*Conn, error) {
	c := &Conn{
		link:       link,
		seg:        seg,
		lanes:      make([]*Lane, lanes),
		ringSize:   ringSize,
		maxRecord:  ring.MaxRecord(ringSize),
		spin:       spin,
		addr:       addr(path),
		sendDir:    dirDialerToListener,
		done:       make(chan struct{}),
		readerDone: make(chan struct{}),
	}
	if listener {
		c.sendDir = dirListenerToDialer
	}
	recvDir := 1 - c.sendDir
	mem := seg.Bytes()
	rs := ring.Size(ringSize)
	region := func(k int) []byte { return mem[k*rs : (k+1)*rs : (k+1)*rs] }
	for k := range 2 * lanes {
		if capacity, err := ring.Verify(region(k)); err != nil || capacity != ringSize {
			return nil, corruptErr(k, err)
		}
	}
	for i := range lanes {
		tx, rx := 2*i+int(c.sendDir), 2*i+int(recvDir)
		prod, err := ring.NewProducer(region(tx))
		if err != nil {
			return nil, corruptErr(tx, err)
		}
		cons, err := ring.NewConsumer(region(rx))
		if err != nil {
			return nil, corruptErr(rx, err)
		}
		c.lanes[i] = &Lane{
			c:    c,
			idx:  i,
			prod: prod,
			cons: cons,
			tx:   half{sig: make(chan struct{}, 1), bell: newDoorbell(i, c.sendDir)},
			rx:   half{sig: make(chan struct{}, 1), bell: newDoorbell(i, recvDir)},
		}
	}
	c.live.Store(int32(2 * lanes))
	go c.readLoop()
	return c, nil
}

func corruptErr(k int, err error) error {
	if err == nil {
		err = errors.New("capacity differs from the negotiated ring size")
	}
	return fmt.Errorf("%w: ring %d: %w", ErrCorrupt, k, err)
}

// readLoop dispatches doorbells to lane signals until the link fails.
func (c *Conn) readLoop() {
	defer close(c.readerDone)
	for {
		typ, p, fds, err := c.link.Recv()
		if err != nil {
			c.shutdown(peerErr(err))
			return
		}
		if len(fds) > 0 {
			discardFDs(fds)
			c.shutdown(errProtocol)
			return
		}
		switch typ {
		case frameDoorbell:
			i, dir, err := decodeDoorbell(p)
			if err != nil || i >= len(c.lanes) {
				c.shutdown(errProtocol)
				return
			}
			h := &c.lanes[i].rx
			if dir == c.sendDir {
				h = &c.lanes[i].tx
			}
			select {
			case h.sig <- struct{}{}:
			default:
			}
		case frameBye:
			c.shutdown(ErrPeerClosed)
			return
		default:
			c.shutdown(errProtocol)
			return
		}
	}
}

// shutdown closes the connection once with err as the reason, waking every
// blocked call; the segment is released when no lane slice is outstanding.
func (c *Conn) shutdown(err error) {
	c.closeOnce.Do(func() {
		c.err = err
		close(c.done)
		if err == ErrClosed {
			c.link.SetWriteDeadline(time.Now().Add(time.Second))
			c.link.Send(frameBye, nil, nil)
		}
		c.link.Close()
		for _, l := range c.lanes {
			l.tx.close(c)
			l.rx.close(c)
		}
	})
}

// halfDone is called once per lane half after close, when it holds no slice.
func (c *Conn) halfDone() {
	if c.live.Add(-1) == 0 {
		c.seg.Close()
		c.unmapped.Store(true)
	}
}

// ring sends a doorbell for a peer parked on the ring bell names.
func (c *Conn) ring(bell *doorbell) error {
	if err := c.link.Send(frameDoorbell, bell[:], nil); err != nil {
		c.shutdown(peerErr(err))
		return c.err
	}
	return nil
}

// peerErr maps the link errors a vanished peer causes to ErrPeerClosed.
func peerErr(err error) error {
	if errors.Is(err, io.EOF) || errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.EPIPE) {
		return ErrPeerClosed
	}
	return err
}

// Lanes returns the number of lanes.
func (c *Conn) Lanes() int { return len(c.lanes) }

// Lane returns lane i, 0 <= i < Lanes().
func (c *Conn) Lane(i int) *Lane { return c.lanes[i] }

// RingSize returns each lane's ring capacity per direction.
func (c *Conn) RingSize() int { return c.ringSize }

// MaxRecord returns the largest record a lane accepts.
func (c *Conn) MaxRecord() int { return c.maxRecord }

// LocalAddr returns the socket path the connection was made on.
func (c *Conn) LocalAddr() net.Addr { return c.addr }

// RemoteAddr returns the socket path the connection was made on.
func (c *Conn) RemoteAddr() net.Addr { return c.addr }

// Close tells the peer goodbye, closes the link and wakes every blocked call
// with ErrClosed. Slices still held from Reserve or Next stay readable until
// they are committed, aborted or released.
func (c *Conn) Close() error {
	c.shutdown(ErrClosed)
	<-c.readerDone
	return nil
}

// Done is closed once the connection is closed by either end or fails.
func (c *Conn) Done() <-chan struct{} { return c.done }

// Err returns nil while the connection is open, then why it closed:
// ErrClosed, ErrPeerClosed, ErrCorrupt or a link error.
func (c *Conn) Err() error {
	select {
	case <-c.done:
		return c.err
	default:
		return nil
	}
}
