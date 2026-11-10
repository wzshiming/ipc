package ipc

import (
	"io"
	"os"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/wzshiming/ipc/internal/ring"
)

// half state: a count of in-progress calls and held slices, plus flags set
// by close and by whoever reports the half done to the Conn.
const (
	halfClosed int32 = 1 << 20
	halfDone   int32 = 1 << 21
)

// spinCheckEvery is how many failed re-checks a spin makes between reads
// of the clock, the deadline and the connection's done channel.
const spinCheckEvery = 128

// half is one direction of a lane as seen by this end: its ring access gate,
// its doorbell signal, the doorbell it sends to the peer and its deadline.
type half struct {
	state atomic.Int32
	held  bool // a Reserve or Next slice is outstanding
	sig   chan struct{}
	bell  doorbell
	dl    deadline
	_     [128 - 24 - unsafe.Sizeof(deadline{})]byte
}

// acquire admits one ring access; it fails once the connection is closed.
func (h *half) acquire(c *Conn) bool {
	if h.state.Add(1)&halfClosed != 0 {
		h.release(c)
		return false
	}
	return true
}

// release ends a ring access and reports whether the connection is closed.
// The last release after close lets the Conn unmap the segment.
func (h *half) release(c *Conn) (closed bool) {
	v := h.state.Add(-1)
	if v == halfClosed && h.state.CompareAndSwap(halfClosed, halfClosed|halfDone) {
		c.halfDone()
	}
	return v&halfClosed != 0
}

func (h *half) close(c *Conn) {
	if h.state.Add(halfClosed) == halfClosed && h.state.CompareAndSwap(halfClosed, halfClosed|halfDone) {
		c.halfDone()
	}
}

// Lane is one bidirectional pair of rings. Each direction may be used by one
// goroutine at a time: one may send while another receives. Sends honour the
// write deadline and receives the read deadline, as on a net.Conn.
//
// Slices returned by Reserve and Next alias shared memory: a Reserve slice is
// valid until Commit or Abort, a Next slice until Release.
type Lane struct {
	c    *Conn
	idx  int
	prod *ring.Producer
	cons *ring.Consumer
	_    [128]byte
	tx   half
	rx   half
}

// Conn returns the connection the lane belongs to.
func (l *Lane) Conn() *Conn { return l.c }

// Index returns the lane's index in its connection.
func (l *Lane) Index() int { return l.idx }

// ringErr maps a ring error to the package's errors, closing the connection
// when the peer corrupted the ring.
func (l *Lane) ringErr(err error) error {
	switch err {
	case ring.ErrFull, ring.ErrEmpty:
		return ErrWouldBlock
	case ring.ErrTooLarge:
		return ErrTooLarge
	}
	l.c.shutdown(ErrCorrupt)
	return ErrCorrupt
}

// TryReserve returns an n-byte slice of the lane's send ring to fill and
// then Commit or Abort, without blocking. It returns ErrWouldBlock when the
// ring has no room for n bytes, ErrTooLarge when n exceeds MaxRecord, and
// ErrPeerClosed or net.ErrClosed once the connection is closed. It ignores
// the write deadline.
func (l *Lane) TryReserve(n int) ([]byte, error) {
	if l.tx.held {
		return nil, errOrder
	}
	if !l.tx.acquire(l.c) {
		return nil, l.c.sendErr()
	}
	p, err := l.prod.TryReserve(n)
	if err != nil {
		l.tx.release(l.c)
		return nil, l.ringErr(err)
	}
	l.tx.held = true
	return p, nil
}

// Reserve is like TryReserve but blocks until the ring has room, the write
// deadline passes or the connection closes. Past the deadline it returns
// os.ErrDeadlineExceeded. It spins for the connection's Spin duration before
// parking until the peer frees room.
func (l *Lane) Reserve(n int) ([]byte, error) {
	if l.tx.dl.expired.Load() {
		return nil, os.ErrDeadlineExceeded
	}
	p, err := l.TryReserve(n)
	if err != ErrWouldBlock {
		return p, err
	}
	if l.c.spin > 0 {
		deadline := time.Now().Add(l.c.spin)
		for i := 1; ; i++ {
			if p, err = l.TryReserve(n); err != ErrWouldBlock {
				return p, err
			}
			if i%spinCheckEvery == 0 {
				if l.tx.dl.expired.Load() {
					return nil, os.ErrDeadlineExceeded
				}
				if isClosed(l.c.done) {
					return nil, l.c.sendErr()
				}
				if !time.Now().Before(deadline) {
					break
				}
			}
		}
	}
	for {
		if !l.tx.acquire(l.c) {
			return nil, l.c.sendErr()
		}
		ready := l.prod.Park(n)
		l.tx.release(l.c)
		if !ready {
			select {
			case <-l.tx.sig:
			case <-l.tx.dl.done():
				if l.tx.acquire(l.c) {
					l.prod.Unpark()
					l.tx.release(l.c)
				}
				return nil, os.ErrDeadlineExceeded
			case <-l.c.done:
				return nil, l.c.sendErr()
			}
		}
		if p, err = l.TryReserve(n); err != ErrWouldBlock {
			return p, err
		}
	}
}

// Commit publishes the record reserved by Reserve or TryReserve to the peer
// and wakes the peer if it is parked. After the connection closed it returns
// ErrPeerClosed or net.ErrClosed, as the peer will not read the record.
func (l *Lane) Commit() error {
	if !l.tx.held {
		return errOrder
	}
	l.tx.held = false
	wake := l.prod.Commit()
	if l.tx.release(l.c) {
		return l.c.sendErr()
	}
	if wake {
		return l.c.ring(&l.tx.bell)
	}
	return nil
}

// Abort discards the record reserved by Reserve or TryReserve, if any.
func (l *Lane) Abort() {
	if !l.tx.held {
		return
	}
	l.tx.held = false
	l.prod.Abort()
	l.tx.release(l.c)
}

// TryNext returns the next record from the lane's receive ring without
// blocking; the slice aliases shared memory until Release. It returns
// ErrWouldBlock when there is no record. After the peer closed it still
// returns the records the peer committed, then io.EOF if the peer closed
// gracefully or ErrPeerClosed if it died; after Close it returns
// net.ErrClosed. It ignores the read deadline.
func (l *Lane) TryNext() ([]byte, error) {
	if l.rx.held {
		return nil, errOrder
	}
	if !l.rx.acquire(l.c) {
		return nil, l.c.recvErr()
	}
	p, err := l.cons.TryNext()
	if err == ring.ErrEmpty && isClosed(l.c.done) {
		// Recheck: all the peer committed before it left is visible by now.
		if p, err = l.cons.TryNext(); err == ring.ErrEmpty {
			l.rx.release(l.c)
			return nil, l.c.recvErr()
		}
	}
	if err != nil {
		l.rx.release(l.c)
		return nil, l.ringErr(err)
	}
	l.rx.held = true
	return p, nil
}

// Next is like TryNext but blocks until a record arrives, the read deadline
// passes or the connection closes. Past the deadline it returns
// os.ErrDeadlineExceeded. After the peer closed it drains without blocking.
// It spins for the connection's Spin duration before parking until the peer
// commits.
func (l *Lane) Next() ([]byte, error) {
	if l.rx.dl.expired.Load() {
		return nil, os.ErrDeadlineExceeded
	}
	p, err := l.TryNext()
	if err != ErrWouldBlock {
		return p, err
	}
	if l.c.spin > 0 {
		deadline := time.Now().Add(l.c.spin)
		for i := 1; ; i++ {
			if p, err = l.TryNext(); err != ErrWouldBlock {
				return p, err
			}
			if i%spinCheckEvery == 0 {
				// No done check: TryNext reports the close once drained.
				if l.rx.dl.expired.Load() {
					return nil, os.ErrDeadlineExceeded
				}
				if !time.Now().Before(deadline) {
					break
				}
			}
		}
	}
	for {
		if !l.rx.acquire(l.c) {
			return nil, l.c.recvErr()
		}
		ready := l.cons.Park()
		l.rx.release(l.c)
		if !ready {
			select {
			case <-l.rx.sig:
			case <-l.rx.dl.done():
				if l.rx.acquire(l.c) {
					l.cons.Unpark()
					l.rx.release(l.c)
				}
				return nil, os.ErrDeadlineExceeded
			case <-l.c.done:
			}
		}
		if p, err = l.TryNext(); err != ErrWouldBlock {
			return p, err
		}
	}
}

// Release frees the record returned by Next or TryNext, making its room
// available to the peer.
func (l *Lane) Release() error {
	if !l.rx.held {
		return errOrder
	}
	l.rx.held = false
	wake := l.cons.Release()
	if l.rx.release(l.c) {
		return nil
	}
	if wake {
		// A failed doorbell means the peer is gone; Next reports that once drained.
		l.c.ring(&l.rx.bell)
	}
	return nil
}

// Write sends p as one record, blocking for room like Reserve, and returns
// len(p) on success. A p longer than MaxRecord is not sent and yields 0,
// ErrTooLarge; an empty p sends a zero-length record.
func (l *Lane) Write(p []byte) (int, error) {
	if len(p) > l.c.maxRecord {
		return 0, ErrTooLarge
	}
	b, err := l.Reserve(len(p))
	if err != nil {
		return 0, err
	}
	copy(b, p)
	if err := l.Commit(); err != nil {
		return 0, err
	}
	return len(p), nil
}

// Read receives one record into p, blocking and draining like Next, and
// returns its length. A record longer than p fills p, is consumed anyway and
// yields len(p), io.ErrShortBuffer.
func (l *Lane) Read(p []byte) (int, error) {
	rec, err := l.Next()
	if err != nil {
		return 0, err
	}
	n := copy(p, rec)
	if err := l.Release(); err != nil {
		return n, err
	}
	if n < len(rec) {
		return n, io.ErrShortBuffer
	}
	return n, nil
}

// SetDeadline sets both the read and the write deadline, as
// SetReadDeadline and SetWriteDeadline do.
func (l *Lane) SetDeadline(t time.Time) error {
	l.rx.dl.set(t)
	l.tx.dl.set(t)
	return nil
}

// SetReadDeadline sets the deadline for Next and Read, including calls
// already blocked. Past the deadline they fail with os.ErrDeadlineExceeded,
// which is a net.Error whose Timeout method reports true; a zero t means no
// deadline. TryNext ignores it.
func (l *Lane) SetReadDeadline(t time.Time) error {
	l.rx.dl.set(t)
	return nil
}

// SetWriteDeadline sets the deadline for Reserve and Write, including calls
// already blocked. Past the deadline they fail with os.ErrDeadlineExceeded;
// a zero t means no deadline. TryReserve ignores it.
func (l *Lane) SetWriteDeadline(t time.Time) error {
	l.tx.dl.set(t)
	return nil
}
