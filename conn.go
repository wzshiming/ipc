package ipc

import (
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/wzshiming/ipc/internal/control"
	"github.com/wzshiming/ipc/internal/ring"
	"github.com/wzshiming/ipc/internal/shm"
)

// addr is the address of a listener or connection: its socket path.
type addr string

func (addr) Network() string  { return "ipc" }
func (a addr) String() string { return string(a) }

// Conn is one shared-memory connection: a pair of rings, one per direction,
// in one segment. It is a net.Conn whose Read and Write copy a byte stream
// over records, and it offers the records themselves through TryReserve,
// Reserve, Commit, Abort, TryNext, Next and Release. Their slices alias
// shared memory: a Reserve slice is valid until Commit or Abort, a Next
// slice until Release. io.Copy into or out of a Conn uses ReadFrom and
// WriteTo, which move the bytes between the ring and the other endpoint
// without a copy.
//
// The stream methods may be called from any goroutine. The record methods of
// one direction serve one goroutine at a time: one may send while another
// receives. On one direction use either the record methods or the stream
// methods, Write and ReadFrom on the send side and Read and WriteTo on the
// receive side; interleaving them while Read or WriteTo holds a partly
// consumed record is not supported. Sends honour the write deadline and
// receives the read deadline.
type Conn struct {
	link      *control.Link
	seg       *shm.Segment
	prod      *ring.Producer
	cons      *ring.Consumer
	ringSize  int
	maxRecord int
	spin      time.Duration
	addr      addr
	sendDir   uint8

	closeOnce  sync.Once
	rxOnce     sync.Once
	done       chan struct{}
	err        error       // written once before done is closed
	closed     atomic.Bool // Close was called on this end
	bye        atomic.Bool // the peer said goodbye
	readerDone chan struct{}
	live       atomic.Int32 // halves that may still touch the segment
	unmapped   atomic.Bool  // test hook: the segment has been released

	wmu sync.Mutex // serializes Write and ReadFrom
	tx  half
	rx  half
	rmu sync.Mutex // serializes Read and WriteTo
	rec []byte     // unread rest of the record Read or WriteTo holds
}

var (
	_ net.Conn      = (*Conn)(nil)
	_ io.ReaderFrom = (*Conn)(nil)
	_ io.WriterTo   = (*Conn)(nil)
)

// newConn wires the two rings of seg and starts the control reader. On error
// the caller still owns link and seg.
func newConn(link *control.Link, seg *shm.Segment, ringSize int, spin time.Duration, path string, listener bool) (*Conn, error) {
	c := &Conn{
		link:       link,
		seg:        seg,
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
	mem, rs := seg.Bytes(), ring.Size(ringSize)
	region := func(dir uint8) []byte {
		k := int(dir)
		return mem[k*rs : (k+1)*rs : (k+1)*rs]
	}
	for dir := range uint8(2) {
		if capacity, err := ring.Verify(region(dir)); err != nil || capacity != ringSize {
			return nil, corruptErr(dir, err)
		}
	}
	var err error
	if c.prod, err = ring.NewProducer(region(c.sendDir)); err != nil {
		return nil, corruptErr(c.sendDir, err)
	}
	if c.cons, err = ring.NewConsumer(region(recvDir)); err != nil {
		return nil, corruptErr(recvDir, err)
	}
	c.tx.sig, c.tx.bell = make(chan struct{}, 1), doorbell{c.sendDir}
	c.rx.sig, c.rx.bell = make(chan struct{}, 1), doorbell{recvDir}
	c.live.Store(2)
	go c.readLoop()
	return c, nil
}

func corruptErr(dir uint8, err error) error {
	if err == nil {
		err = errors.New("capacity differs from the negotiated ring size")
	}
	return fmt.Errorf("%w: ring %d: %w", ErrCorrupt, dir, err)
}

// readLoop dispatches doorbells to the halves until the link fails.
func (c *Conn) readLoop() {
	defer close(c.readerDone)
	for {
		typ, p, fds, err := c.link.Recv()
		if err != nil {
			c.peerGone(peerErr(err))
			return
		}
		if len(fds) > 0 {
			discardFDs(fds)
			c.shutdown(errProtocol)
			return
		}
		switch typ {
		case frameDoorbell:
			dir, err := decodeDoorbell(p)
			if err != nil {
				c.shutdown(errProtocol)
				return
			}
			h := &c.rx
			if dir == c.sendDir {
				h = &c.tx
			}
			select {
			case h.sig <- struct{}{}:
			default:
			}
		case frameBye:
			c.bye.Store(true)
			c.peerGone(ErrPeerClosed)
			return
		default:
			c.shutdown(errProtocol)
			return
		}
	}
}

// shutdown closes the connection with err as the reason unless it is closed
// already, waking every blocked call and ending receives; the segment is
// released when no slice is outstanding.
func (c *Conn) shutdown(err error) {
	c.peerGone(err)
	c.rxOnce.Do(func() { c.rx.close(c) })
}

// peerGone closes the connection like shutdown but leaves the receive half
// open, so records the peer committed can still be drained until Close.
func (c *Conn) peerGone(err error) {
	c.closeOnce.Do(func() {
		c.err = err
		// Sends fail before Done closes, so callers seeing Done see them fail.
		c.tx.close(c)
		close(c.done)
		if err == net.ErrClosed {
			c.link.SetWriteDeadline(time.Now().Add(time.Second))
			c.link.Send(frameBye, nil, nil)
		}
		c.link.Close()
	})
}

// halfDone is called once per half after close, when it holds no slice.
func (c *Conn) halfDone() {
	if c.live.Add(-1) == 0 {
		c.seg.Close()
		c.unmapped.Store(true)
	}
}

// ring sends a doorbell for a peer parked on the ring bell names.
func (c *Conn) ring(bell *doorbell) error {
	if err := c.link.Send(frameDoorbell, bell[:], nil); err != nil {
		c.peerGone(peerErr(err))
		return c.sendErr()
	}
	return nil
}

// sendErr is what sends return once the connection is closed.
func (c *Conn) sendErr() error {
	if c.closed.Load() {
		return net.ErrClosed
	}
	return c.err
}

// recvErr is what receives return once the connection is closed and the
// records the peer committed are drained.
func (c *Conn) recvErr() error {
	if c.closed.Load() {
		return net.ErrClosed
	}
	if c.err == ErrPeerClosed && c.bye.Load() {
		return io.EOF
	}
	return c.err
}

// timeout is what a call past its deadline returns, net.ErrClosed once Close
// was called.
func (c *Conn) timeout() error {
	if c.closed.Load() {
		return net.ErrClosed
	}
	return os.ErrDeadlineExceeded
}

// peerErr maps the link errors a vanished peer causes to ErrPeerClosed.
func peerErr(err error) error {
	if isPeerGone(err) {
		return ErrPeerClosed
	}
	return err
}

// RingSize returns the ring capacity per direction.
func (c *Conn) RingSize() int { return c.ringSize }

// MaxRecord returns the largest record the connection accepts.
func (c *Conn) MaxRecord() int { return c.maxRecord }

// LocalAddr returns the socket path the connection was made on.
func (c *Conn) LocalAddr() net.Addr { return c.addr }

// RemoteAddr returns the socket path the connection was made on.
func (c *Conn) RemoteAddr() net.Addr { return c.addr }

// SetDeadline sets both the read and the write deadline, as
// SetReadDeadline and SetWriteDeadline do.
func (c *Conn) SetDeadline(t time.Time) error {
	c.rx.dl.set(t)
	c.tx.dl.set(t)
	return nil
}

// SetReadDeadline sets the deadline for Read, WriteTo and Next, including
// calls already blocked. Past the deadline they fail with
// os.ErrDeadlineExceeded, which is a net.Error whose Timeout method reports
// true, or with net.ErrClosed after Close; a zero t means no deadline.
// TryNext ignores it.
func (c *Conn) SetReadDeadline(t time.Time) error {
	c.rx.dl.set(t)
	return nil
}

// SetWriteDeadline sets the deadline for Write, ReadFrom and Reserve,
// including calls already blocked. Past the deadline they fail with
// os.ErrDeadlineExceeded, or with net.ErrClosed after Close; a zero t means
// no deadline. TryReserve ignores it.
func (c *Conn) SetWriteDeadline(t time.Time) error {
	c.tx.dl.set(t)
	return nil
}

// Close tells the peer goodbye, closes the link and wakes every blocked call
// with net.ErrClosed, which later sends and receives return too, even past
// their deadline; records the peer committed are no longer delivered. It
// does not wait for a stream call in progress, such as a WriteTo inside its
// destination's Write. Slices still held from Reserve or Next stay readable
// until they are committed, aborted or released, while a record Read or
// WriteTo holds partly consumed is released, at once or by the call in
// progress when it returns; the segment is unmapped once no slice is left.
func (c *Conn) Close() error {
	c.closed.Store(true)
	c.shutdown(net.ErrClosed)
	<-c.readerDone
	if c.rmu.TryLock() {
		c.drop()
		c.rmu.Unlock()
	}
	return nil
}

// Done is closed once the connection is closed by either end or fails. After
// the peer's close, receives still drain what it committed until Close.
func (c *Conn) Done() <-chan struct{} { return c.done }

// Err returns nil while the connection is open, then why it closed:
// net.ErrClosed after Close, ErrPeerClosed after the peer closed or died,
// else ErrCorrupt or the link error.
func (c *Conn) Err() error {
	select {
	case <-c.done:
		return c.sendErr()
	default:
		return nil
	}
}
