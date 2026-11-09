package ipc

import (
	"context"
	"io"
	"sync/atomic"
	"time"

	"github.com/wzshiming/ipc/internal/ring"
)

// half state: a count of in-progress calls and held slices, plus flags set
// by close and by whoever reports the half done to the Conn.
const (
	halfClosed int32 = 1 << 20
	halfDone   int32 = 1 << 21
)

// spinCheckEvery is how many failed re-checks a spin makes between reads
// of the clock, ctx and the connection's done channel.
const spinCheckEvery = 128

// half is one direction of a lane as seen by this end: its ring access gate,
// its doorbell signal and the doorbell it sends to the peer.
type half struct {
	state atomic.Int32
	held  bool // a Reserve or Next slice is outstanding
	sig   chan struct{}
	bell  doorbell
	_     [128 - 24]byte
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
// goroutine at a time: one may send while another receives.
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

// TryReserve returns an n-byte slice to fill and Commit, or ErrWouldBlock
// when the lane has no room for it.
func (l *Lane) TryReserve(n int) ([]byte, error) {
	if l.tx.held {
		return nil, errOrder
	}
	if !l.tx.acquire(l.c) {
		return nil, l.c.err
	}
	p, err := l.prod.TryReserve(n)
	if err != nil {
		l.tx.release(l.c)
		return nil, l.ringErr(err)
	}
	l.tx.held = true
	return p, nil
}

// Reserve is TryReserve that waits for room until ctx is done or the
// connection closes.
func (l *Lane) Reserve(ctx context.Context, n int) ([]byte, error) {
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
				if err := l.c.interrupted(ctx); err != nil {
					return nil, err
				}
				if !time.Now().Before(deadline) {
					break
				}
			}
		}
	}
	for {
		if !l.tx.acquire(l.c) {
			return nil, l.c.err
		}
		ready := l.prod.Park(n)
		l.tx.release(l.c)
		if !ready {
			select {
			case <-l.tx.sig:
			case <-ctx.Done():
				if l.tx.acquire(l.c) {
					l.prod.Unpark()
					l.tx.release(l.c)
				}
				return nil, ctx.Err()
			case <-l.c.done:
				return nil, l.c.err
			}
		}
		if p, err = l.TryReserve(n); err != ErrWouldBlock {
			return p, err
		}
	}
}

// Commit publishes the reserved record. After the connection closed it
// returns the close reason, as the peer will not read the record.
func (l *Lane) Commit() error {
	if !l.tx.held {
		return errOrder
	}
	l.tx.held = false
	wake := l.prod.Commit()
	if l.tx.release(l.c) {
		return l.c.err
	}
	if wake {
		return l.c.ring(&l.tx.bell)
	}
	return nil
}

// Abort discards the reserved record, if any.
func (l *Lane) Abort() {
	if !l.tx.held {
		return
	}
	l.tx.held = false
	l.prod.Abort()
	l.tx.release(l.c)
}

// TryNext returns the next record, or ErrWouldBlock when there is none.
func (l *Lane) TryNext() ([]byte, error) {
	if l.rx.held {
		return nil, errOrder
	}
	if !l.rx.acquire(l.c) {
		return nil, l.c.err
	}
	p, err := l.cons.TryNext()
	if err != nil {
		l.rx.release(l.c)
		return nil, l.ringErr(err)
	}
	l.rx.held = true
	return p, nil
}

// Next is TryNext that waits for a record until ctx is done or the
// connection closes.
func (l *Lane) Next(ctx context.Context) ([]byte, error) {
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
				if err := l.c.interrupted(ctx); err != nil {
					return nil, err
				}
				if !time.Now().Before(deadline) {
					break
				}
			}
		}
	}
	for {
		if !l.rx.acquire(l.c) {
			return nil, l.c.err
		}
		ready := l.cons.Park()
		l.rx.release(l.c)
		if !ready {
			select {
			case <-l.rx.sig:
			case <-ctx.Done():
				if l.rx.acquire(l.c) {
					l.cons.Unpark()
					l.rx.release(l.c)
				}
				return nil, ctx.Err()
			case <-l.c.done:
				return nil, l.c.err
			}
		}
		if p, err = l.TryNext(); err != ErrWouldBlock {
			return p, err
		}
	}
}

// Release frees the record returned by Next or TryNext.
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
		return l.c.ring(&l.rx.bell)
	}
	return nil
}

// Send copies p into one record, waiting for room like Reserve.
func (l *Lane) Send(ctx context.Context, p []byte) error {
	if len(p) > l.c.maxRecord {
		return ErrTooLarge
	}
	b, err := l.Reserve(ctx, len(p))
	if err != nil {
		return err
	}
	copy(b, p)
	return l.Commit()
}

// Recv copies the next record into p, waiting like Next. A record longer
// than p fills p, is consumed anyway and yields io.ErrShortBuffer.
func (l *Lane) Recv(ctx context.Context, p []byte) (int, error) {
	rec, err := l.Next(ctx)
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

// interrupted returns why a spinning call must stop, or nil.
func (c *Conn) interrupted(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-c.done:
		return c.err
	default:
		return nil
	}
}
