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
	rxClosed   chan struct{} // closed when receives end: at Close or a failure, not the peer's close
	err        error         // written once before done is closed
	unsettled  bool          // written with err: the reader has yet to learn whether the peer said goodbye
	closed     atomic.Bool   // Close was called on this end
	bye        atomic.Bool   // the peer said goodbye
	readerDone chan struct{}
	live       atomic.Int32 // halves that may still touch the segment
	unmapped   atomic.Bool  // test hook: the segment has been released

	wgate gate // serializes Write and ReadFrom
	tx    half
	rx    half
	rgate gate   // serializes Read and WriteTo
	rec   []byte // unread rest of the record Read or WriteTo holds
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
		rxClosed:   make(chan struct{}),
		readerDone: make(chan struct{}),
		wgate:      gate{baton: make(chan struct{}, 1)},
		rgate:      gate{baton: make(chan struct{}, 1)},
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

// readLoop dispatches doorbells to the halves until the link fails or the
// peer says goodbye, then closes the link. Close leaves it reading until the
// peer closes its side or lingerWait has passed.
func (c *Conn) readLoop() {
	defer close(c.readerDone)
	defer c.link.Close()
	for {
		typ, p, fds, err := c.link.Recv()
		if err != nil {
			c.end(peerErr(err), false)
			return
		}
		if len(fds) > 0 {
			control.CloseFDs(fds)
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
			c.end(ErrPeerClosed, false)
			return
		default:
			c.shutdown(errProtocol)
			return
		}
	}
}

// shutdown closes the connection with err as the reason unless it is closed
// already, then the link and receives, waking every blocked call; the
// segment is released when no slice is outstanding.
func (c *Conn) shutdown(err error) {
	c.end(err, false)
	c.link.Close()
	c.closeRecv()
}

// closeRecv ends receives, waking a blocked one; the segment is released when
// no slice is outstanding.
func (c *Conn) closeRecv() {
	c.rxOnce.Do(func() {
		c.rx.close(c)
		close(c.rxClosed)
	})
}

// end closes the connection for sends with err as the reason unless it is
// closed already, and reports whether it did. Receives still drain what the
// peer committed until Close; unsettled makes them wait for the reader to
// learn whether the peer said goodbye before they end.
func (c *Conn) end(err error, unsettled bool) (ended bool) {
	c.closeOnce.Do(func() {
		c.err, c.unsettled = err, unsettled
		// Sends fail before Done closes, so callers seeing Done see them fail.
		c.tx.close(c)
		close(c.done)
		ended = true
	})
	return ended
}

// halfDone is called once per half after close, when it holds no slice.
func (c *Conn) halfDone() {
	if c.live.Add(-1) == 0 {
		c.seg.Close()
		c.unmapped.Store(true)
	}
}

// byeWait bounds how long the reader may take to learn whether a peer whose
// socket refused a doorbell said goodbye first.
const byeWait = time.Second

// ring sends a doorbell for a peer parked on the ring bell names, waiting no
// longer than TrySend does. A socket that does not take it means the peer
// stopped reading, a protocol violation. A socket the peer closed ends sends
// at once, while the reader reads on, for at most byeWait, to learn whether
// the peer said goodbye first.
func (c *Conn) ring(bell *doorbell) {
	err := c.link.TrySend(frameDoorbell, bell[:])
	switch {
	case err == nil:
	case err == control.ErrBusy:
		c.shutdown(errProtocol)
	case isPeerGone(err):
		if c.end(ErrPeerClosed, true) {
			c.link.SetReadDeadline(time.Now().Add(byeWait))
		}
	default:
		c.end(err, false)
		c.link.Close()
	}
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

// settled reports whether receives end once the ring is drained: the
// connection is closed and the reader knows whether the peer said goodbye.
func (c *Conn) settled() bool {
	return isClosed(c.done) && (!c.unsettled || isClosed(c.readerDone))
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

// Close tells the peer goodbye, giving up after 50 ms when the peer's socket
// does not take it, and wakes every blocked call with net.ErrClosed, which
// later sends and receives return too, even past their deadline; records the
// peer committed are no longer delivered. It then waits up to 50 ms for the
// peer to close its side, as a peer does on reading the goodbye, so that the
// socket is closed when Close returns; a socket the peer keeps open outlives
// Close by a second at most. It never waits for a stream call in progress,
// such as a WriteTo inside its destination's Write. Slices still held from
// Reserve or Next stay readable until they are committed, aborted or
// released, while a record Read or WriteTo holds partly consumed is released,
// at once or by the call in progress when it returns; the segment is
// unmapped once no slice is left.
func (c *Conn) Close() error { return c.closeWaiting(readerWait) }

// closeWaiting is Close, waiting at most wait for the control reader to end.
func (c *Conn) closeWaiting(wait time.Duration) error {
	c.closed.Store(true)
	if c.end(net.ErrClosed, false) {
		// Closing with doorbells unread resets the socket on Windows, losing the
		// goodbye; the reader closes it once the peer closed its side.
		c.link.SetReadDeadline(time.Now().Add(lingerWait))
		c.link.TrySend(frameBye, nil)
		c.link.CloseWrite()
	}
	c.closeRecv()
	if c.rgate.tryLock() {
		c.drop()
		c.rgate.unlock()
	}
	// Exiting right after Close must not close the socket under an unread goodbye.
	select {
	case <-c.readerDone:
	case <-time.After(wait):
	}
	return nil
}

// lingerWait bounds how long the socket outlives Close for the peer to read
// the goodbye and close its side.
const lingerWait = time.Second

// readerWait bounds how long Close waits for the control reader to end.
const readerWait = 50 * time.Millisecond

// Done is closed once the connection is closed by either end or fails. After
// the peer's close, receives still drain what it committed until Close.
func (c *Conn) Done() <-chan struct{} { return c.done }

// Err returns nil while the connection is open, then why it closed:
// net.ErrClosed after Close, ErrPeerClosed after the peer closed or died,
// else ErrCorrupt, a control protocol violation, as by a peer that stopped
// reading its socket, or the link error.
func (c *Conn) Err() error {
	select {
	case <-c.done:
		return c.sendErr()
	default:
		return nil
	}
}
