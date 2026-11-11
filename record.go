package ipc

import (
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

// half is one direction of a connection as seen by this end: its ring access
// gate, its doorbell signal, the doorbell it sends to the peer and its
// deadline.
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
// os.ErrDeadlineExceeded unless Close was called. It spins for the
// connection's Spin duration before parking until the peer frees room.
func (c *Conn) Reserve(n int) ([]byte, error) {
	if c.tx.dl.expired.Load() {
		return nil, c.timeout()
	}
	p, err := c.TryReserve(n)
	if err != ErrWouldBlock {
		return p, err
	}
	if c.spin > 0 {
		deadline := time.Now().Add(c.spin)
		for i := 1; ; i++ {
			if p, err = c.TryReserve(n); err != ErrWouldBlock {
				return p, err
			}
			if i%spinCheckEvery == 0 {
				if c.tx.dl.expired.Load() {
					return nil, c.timeout()
				}
				if isClosed(c.done) {
					return nil, c.sendErr()
				}
				if !time.Now().Before(deadline) {
					break
				}
			}
		}
	}
	for {
		if !c.tx.acquire(c) {
			return nil, c.sendErr()
		}
		ready := c.prod.Park(n)
		c.tx.release(c)
		if !ready {
			select {
			case <-c.tx.sig:
			case <-c.tx.dl.done():
				if c.tx.acquire(c) {
					c.prod.Unpark()
					c.tx.release(c)
				}
				return nil, c.timeout()
			case <-c.done:
				return nil, c.sendErr()
			}
		}
		if p, err = c.TryReserve(n); err != ErrWouldBlock {
			return p, err
		}
	}
}

// Commit publishes the record reserved by Reserve or TryReserve to the peer
// and wakes the peer if it is parked. It returns nil exactly when it
// published the record: once the connection is closed it publishes nothing
// and returns ErrPeerClosed or net.ErrClosed.
func (c *Conn) Commit() error {
	if !c.tx.held {
		return errOrder
	}
	if err := c.abortIfClosed(); err != nil {
		return err
	}
	c.committed(c.prod.Commit())
	return nil
}

// commitN publishes the first n bytes of the reserved record, like Commit.
func (c *Conn) commitN(n int) error {
	if err := c.abortIfClosed(); err != nil {
		return err
	}
	c.committed(c.prod.CommitN(n))
	return nil
}

// abortIfClosed aborts the reservation once the connection is closed and
// returns why, so that the peer never sees a record whose Commit failed.
func (c *Conn) abortIfClosed() error {
	if c.tx.state.Load()&halfClosed == 0 {
		return nil
	}
	c.Abort()
	return c.sendErr()
}

// committed ends the reservation the ring published and rings a parked peer.
func (c *Conn) committed(wake bool) {
	c.tx.held = false
	if !c.tx.release(c) && wake {
		c.ring(&c.tx.bell)
	}
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
// ErrPeerClosed if it died, ErrWouldBlock while that is not yet known; after
// Close it returns net.ErrClosed. It ignores the read deadline.
func (c *Conn) TryNext() ([]byte, error) {
	if c.rx.held {
		return nil, errOrder
	}
	if !c.rx.acquire(c) {
		return nil, c.recvErr()
	}
	p, err := c.cons.TryNext()
	if err == ring.ErrEmpty && c.settled() {
		// Recheck: all the peer committed before it left is visible by now.
		if p, err = c.cons.TryNext(); err == ring.ErrEmpty {
			c.rx.release(c)
			return nil, c.recvErr()
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
// os.ErrDeadlineExceeded unless Close was called. After the peer closed it
// drains without blocking, save for up to a second in which the connection
// learns whether the peer said goodbye. It spins for the connection's Spin
// duration before parking until the peer commits.
func (c *Conn) Next() ([]byte, error) {
	if c.rx.dl.expired.Load() {
		return nil, c.timeout()
	}
	p, err := c.TryNext()
	if err != ErrWouldBlock {
		return p, err
	}
	if c.spin > 0 {
		deadline := time.Now().Add(c.spin)
		for i := 1; ; i++ {
			if p, err = c.TryNext(); err != ErrWouldBlock {
				return p, err
			}
			if i%spinCheckEvery == 0 {
				// No done check: TryNext reports the close once drained.
				if c.rx.dl.expired.Load() {
					return nil, c.timeout()
				}
				if !time.Now().Before(deadline) {
					break
				}
			}
		}
	}
	for {
		if !c.rx.acquire(c) {
			return nil, c.recvErr()
		}
		ready := c.cons.Park()
		c.rx.release(c)
		if !ready {
			select {
			case <-c.rx.sig:
			case <-c.rx.dl.done():
				if c.rx.acquire(c) {
					c.cons.Unpark()
					c.rx.release(c)
				}
				return nil, c.timeout()
			case <-c.readerDone:
			case <-c.rxClosed:
			}
		}
		if p, err = c.TryNext(); err != ErrWouldBlock {
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
	if !c.rx.release(c) && wake {
		c.ring(&c.rx.bell)
	}
	return nil
}
