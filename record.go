package ipc

import (
	"context"
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
// of the clock and the deadline.
const spinCheckEvery = 128

// half is one direction of a connection as seen by this end: its ring access
// gate, its doorbell signal, the doorbell it sends to the peer and its
// deadline.
type half struct {
	state atomic.Int32
	held  bool // a Reserve or Next slice is outstanding
	sig   chan struct{}
	bell  doorbell
	dl    atomic.Pointer[deadlineCtx] // nil for no deadline
	_     [spinCheckEvery - 24 - unsafe.Sizeof(atomic.Pointer[deadlineCtx]{})]byte
}

// deadlineCtx is a half's deadline: ctx ends once the deadline passes or once
// a later one replaces it and calls cancel.
type deadlineCtx struct {
	ctx    context.Context
	cancel context.CancelFunc
}

// setDeadline makes t, zero for none, the half's deadline, waking the calls
// that wait on the one it replaces.
func (h *half) setDeadline(t time.Time) {
	var d *deadlineCtx
	if !t.IsZero() {
		d = new(deadlineCtx)
		d.ctx, d.cancel = context.WithDeadline(context.Background(), t)
	}
	if old := h.dl.Swap(d); old != nil {
		old.cancel()
	} else if d != nil {
		// A call parked without a deadline waits on sig alone.
		select {
		case h.sig <- struct{}{}:
		default:
		}
	}
}

// passed reports whether d has passed.
func (d *deadlineCtx) passed() bool {
	return d.ctx.Err() == context.DeadlineExceeded
}

// done returns the channel closed once d passes or is replaced; nil if d is nil.
func (d *deadlineCtx) done() <-chan struct{} {
	if d == nil {
		return nil
	}
	return d.ctx.Done()
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
	if h.state.Or(halfClosed) == 0 && h.state.CompareAndSwap(halfClosed, halfClosed|halfDone) {
		c.halfDone()
	}
}

// ringErr maps a ring error to the package's errors, closing the connection
// when the peer corrupted the ring.
func (c *Conn) ringErr(err error) error {
	switch err {
	case ring.ErrFull, ring.ErrEmpty:
		return ErrWouldBlock
	case ring.ErrTooLarge:
		return ErrTooLarge
	}
	c.shutdown(ErrCorrupt)
	return ErrCorrupt
}

// TryReserve returns an n-byte slice of the send ring to fill and then
// Commit or Abort, without blocking. It returns ErrWouldBlock when the ring
// has no room for n bytes, ErrTooLarge when n exceeds MaxRecord, and
// ErrPeerClosed or net.ErrClosed once the connection is closed. It ignores
// the write deadline.
func (c *Conn) TryReserve(n int) ([]byte, error) {
	if c.tx.held {
		return nil, errOrder
	}
	if !c.tx.acquire(c) {
		return nil, c.sendErr()
	}
	p, err := c.prod.TryReserve(n)
	if err != nil {
		c.tx.release(c)
		return nil, c.ringErr(err)
	}
	c.tx.held = true
	return p, nil
}

// Reserve is like TryReserve but blocks until the ring has room, the write
// deadline passes or the connection closes. Past the deadline it returns
// os.ErrDeadlineExceeded. It spins for the connection's Spin duration before
// parking until the peer frees room.
func (c *Conn) Reserve(n int) ([]byte, error) {
	if dl := c.tx.dl.Load(); dl != nil && dl.passed() {
		return nil, os.ErrDeadlineExceeded
	}
	p, err := c.TryReserve(n)
	if err != ErrWouldBlock {
		return p, err
	}
	return c.wait(&c.tx, n)
}

// Commit publishes the record reserved by Reserve or TryReserve to the peer
// and wakes the peer if it is parked. After the connection closed it returns
// ErrPeerClosed or net.ErrClosed, as the peer will not read the record.
func (c *Conn) Commit() error {
	if !c.tx.held {
		return errOrder
	}
	return c.committed(c.prod.Commit())
}

// commitN publishes the first n bytes of the reserved record, like Commit.
func (c *Conn) commitN(n int) error {
	return c.committed(c.prod.CommitN(n))
}

// committed ends the reservation the ring published and rings a parked peer.
func (c *Conn) committed(wake bool) error {
	c.tx.held = false
	if c.tx.release(c) {
		return c.sendErr()
	}
	if wake {
		return c.ring(&c.tx.bell)
	}
	return nil
}

// Abort discards the record reserved by Reserve or TryReserve, if any.
func (c *Conn) Abort() {
	if !c.tx.held {
		return
	}
	c.tx.held = false
	c.prod.Abort()
	c.tx.release(c)
}

// TryNext returns the next record from the receive ring without blocking;
// the slice aliases shared memory until Release. It returns ErrWouldBlock
// when there is no record. After the peer closed it still returns the
// records the peer committed, then io.EOF if the peer closed gracefully or
// ErrPeerClosed if it died; after Close it returns net.ErrClosed. It ignores
// the read deadline.
func (c *Conn) TryNext() ([]byte, error) {
	if c.rx.held {
		return nil, errOrder
	}
	if !c.rx.acquire(c) {
		return nil, c.recvErr()
	}
	p, err := c.cons.TryNext()
	if err == ring.ErrEmpty {
		// Nested rather than &&, which spills p and err even when a record came.
		if c.life.Err() != nil {
			// Recheck: all the peer committed before it left is visible by now.
			if p, err = c.cons.TryNext(); err == ring.ErrEmpty {
				c.rx.release(c)
				return nil, c.recvErr()
			}
		}
	}
	if err != nil {
		c.rx.release(c)
		return nil, c.ringErr(err)
	}
	c.rx.held = true
	return p, nil
}

// Next is like TryNext but blocks until a record arrives, the read deadline
// passes or the connection closes. Past the deadline it returns
// os.ErrDeadlineExceeded. After the peer closed it drains without blocking.
// It spins for the connection's Spin duration before parking until the peer
// commits.
func (c *Conn) Next() ([]byte, error) {
	if dl := c.rx.dl.Load(); dl != nil && dl.passed() {
		return nil, os.ErrDeadlineExceeded
	}
	p, err := c.TryNext()
	if err != ErrWouldBlock {
		return p, err
	}
	return c.wait(&c.rx, 0)
}

// wait blocks for Reserve(n) when h is the send half, else for Next.
func (c *Conn) wait(h *half, n int) (p []byte, err error) {
	send := h == &c.tx
	if c.spin > 0 {
		deadline := time.Now().Add(c.spin)
		for i := 1; ; i++ {
			if send {
				p, err = c.TryReserve(n)
			} else {
				p, err = c.TryNext()
			}
			if err != ErrWouldBlock {
				return p, err
			}
			if i%spinCheckEvery == 0 {
				// No done check: the try call reports the close, TryNext once drained.
				if dl := h.dl.Load(); dl != nil && dl.passed() {
					return nil, os.ErrDeadlineExceeded
				}
				if !time.Now().Before(deadline) {
					break
				}
			}
		}
	}
	for {
		if !h.acquire(c) {
			if send {
				return nil, c.sendErr()
			}
			return nil, c.recvErr()
		}
		var ready bool
		if send {
			ready = c.prod.Park(n)
		} else {
			ready = c.cons.Park()
		}
		h.release(c)
		if !ready {
			dl := h.dl.Load()
			select {
			case <-h.sig:
			case <-dl.done():
				if !dl.passed() {
					continue // replaced: wait under the new deadline
				}
				if h.acquire(c) {
					if send {
						c.prod.Unpark()
					} else {
						c.cons.Unpark()
					}
					h.release(c)
				}
				return nil, os.ErrDeadlineExceeded
			case <-c.life.Done():
			}
		}
		if send {
			p, err = c.TryReserve(n)
		} else {
			p, err = c.TryNext()
		}
		if err != ErrWouldBlock {
			return p, err
		}
	}
}

// Release frees the record returned by Next or TryNext, making its room
// available to the peer.
func (c *Conn) Release() error {
	if !c.rx.held {
		return errOrder
	}
	c.rx.held = false
	wake := c.cons.Release()
	if c.rx.release(c) {
		return nil
	}
	if wake {
		// A failed doorbell means the peer is gone; Next reports that once drained.
		c.ring(&c.rx.bell)
	}
	return nil
}
