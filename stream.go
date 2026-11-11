package ipc

import (
	"io"
	"net"
	"os"
	"sync/atomic"
	"time"
)

// Read copies the next bytes of the stream into p: the records the peer
// committed, in order, a long one continued by later Reads and a zero-length
// one skipped. It blocks like Next and, once the records are drained, reports
// the peer's close as io.EOF or its death as ErrPeerClosed. Past the read
// deadline it fails without consuming anything.
func (c *Conn) Read(p []byte) (int, error) {
	if err := c.lockRecv(); err != nil {
		return 0, err
	}
	defer c.unlockRecv()
	if len(p) == 0 {
		return 0, nil
	}
	if err := c.hold(); err != nil {
		return 0, err
	}
	n := copy(p, c.rec)
	c.consume(n)
	return n, nil
}

// hold makes c.rec the unread rest of a record, taking the next non-empty
// one from the peer when none is held.
func (c *Conn) hold() error {
	for len(c.rec) == 0 {
		rec, err := c.Next()
		if err != nil {
			return err
		}
		if len(rec) == 0 {
			c.Release()
			continue
		}
		c.rec = rec
	}
	return nil
}

// consume marks the first n bytes of the held record read, releasing the
// record once all of it is.
func (c *Conn) consume(n int) {
	if c.rec = c.rec[n:]; len(c.rec) == 0 {
		c.Release()
	}
}

// drop releases the record held partly consumed, if any.
func (c *Conn) drop() {
	if len(c.rec) > 0 {
		c.rec = nil
		c.Release()
	}
}

// lockRecv admits a Read or WriteTo once the one before it has returned.
// Waiting ends at the read deadline or Close but not at the peer's close, as
// the stream can still be drained.
func (c *Conn) lockRecv() error {
	if !c.rgate.lock(c.rxClosed, &c.rx.dl) {
		if c.rx.dl.expired.Load() || !isClosed(c.rxClosed) {
			return c.timeout()
		}
		return c.recvErr()
	}
	if c.closed.Load() {
		c.unlockRecv()
		return net.ErrClosed
	}
	if c.rx.dl.expired.Load() {
		c.unlockRecv()
		return os.ErrDeadlineExceeded
	}
	return nil
}

// unlockRecv ends a Read or WriteTo. Close leaves the held record to a call
// in progress, so after a Close the call drops it; checking after the unlock
// misses no Close that found the gate taken.
func (c *Conn) unlockRecv() {
	c.rgate.unlock()
	if c.closed.Load() && c.rgate.tryLock() {
		c.drop()
		c.rgate.unlock()
	}
}

// WriteTo writes the stream to w without copying: the unread rest of a record
// Read holds, then every record's slice straight from the ring, each released
// after w.Write returns. It returns n and nil once the peer's close is
// drained, else Read's errors, w's error or io.ErrShortWrite, keeping the
// unwritten rest of the record for a later Read or WriteTo.
func (c *Conn) WriteTo(w io.Writer) (int64, error) {
	if err := c.lockRecv(); err != nil {
		return 0, err
	}
	defer c.unlockRecv()
	var n int64
	for {
		if err := c.hold(); err != nil {
			if err == io.EOF {
				err = nil
			}
			return n, err
		}
		k, err := w.Write(c.rec)
		n += int64(k)
		c.consume(k)
		if err == nil && len(c.rec) > 0 {
			err = io.ErrShortWrite
		}
		if err != nil {
			return n, err
		}
	}
}

// Write copies p into records of at most MaxRecord bytes, blocking for room
// like Reserve, and returns how many bytes it sent. An empty p sends nothing.
func (c *Conn) Write(p []byte) (int, error) {
	if err := c.lockSend(); err != nil {
		return 0, err
	}
	defer c.wgate.unlock()
	n := 0
	for n < len(p) {
		k := min(len(p)-n, c.maxRecord)
		b, err := c.Reserve(k)
		if err != nil {
			return n, err
		}
		copy(b, p[n:n+k])
		if err := c.Commit(); err != nil {
			return n, err
		}
		n += k
	}
	return n, nil
}

// lockSend admits a Write or ReadFrom once the one before it has returned.
// Waiting ends at the write deadline or when either end closes.
func (c *Conn) lockSend() error {
	if !c.wgate.lock(c.done, &c.tx.dl) {
		if c.tx.dl.expired.Load() || !isClosed(c.done) {
			return c.timeout()
		}
		return c.sendErr()
	}
	if c.closed.Load() {
		c.wgate.unlock()
		return net.ErrClosed
	}
	return nil
}

// minRun is the least ReadFrom reserves; with less free it waits for room.
const minRun = 4 << 10

// ReadFrom sends what r reads without copying: each Read of r fills a record
// in place, reserved in the ring's free run up to MaxRecord, and exactly the
// bytes it returns are committed; with less than 4 KiB free it blocks for
// room like Reserve. It returns n and nil once r reports io.EOF, else r's
// error or Write's. When the peer's close stops it, it still asks r for
// more, as a copy into a socket would: n and nil when r is exhausted, else
// ErrPeerClosed, with what that Read returned dropped.
func (c *Conn) ReadFrom(r io.Reader) (int64, error) {
	if err := c.lockSend(); err != nil {
		return 0, err
	}
	defer c.wgate.unlock()
	var n int64
	for {
		b, err := c.Reserve(max(c.room(), minRun))
		if err != nil {
			if err == ErrPeerClosed && exhausted(r) {
				return n, nil
			}
			return n, err
		}
		k, rerr := r.Read(b)
		if k == 0 {
			c.Abort()
		} else if err := c.commitN(k); err != nil {
			return n, err
		}
		n += int64(k)
		if rerr == io.EOF {
			return n, nil
		}
		if rerr != nil {
			return n, rerr
		}
	}
}

// room is the largest record the send ring takes now, 0 once closed.
func (c *Conn) room() int {
	if !c.tx.acquire(c) {
		return 0
	}
	n := c.prod.Room()
	c.tx.release(c)
	return n
}

// exhausted reports whether r has nothing left: its next Read, into one byte
// of private memory since the peer's close took the ring away, returns
// io.EOF alone. A byte read instead is lost, as the rest of r is.
func exhausted(r io.Reader) bool {
	var b [1]byte
	for {
		k, err := r.Read(b[:])
		if k > 0 || err != nil {
			return k == 0 && err == io.EOF
		}
	}
}

// gate serializes the stream calls of one direction like a sync.Mutex: a call
// takes a free gate ahead of waiting ones, sparing contending calls a
// goroutine switch each, unless one has waited longer than starveAfter. A
// waiting call can give up.
type gate struct {
	state atomic.Int64  // gateLocked | gateWoken | waiting calls | starving calls
	baton chan struct{} // wakes one waiting call to contend for the gate
}

const (
	gateLocked   int64 = 1                  // a call holds the gate
	gateWoken    int64 = 2                  // a woken waiting call has yet to contend
	gateWaiter   int64 = 4                  // one waiting call
	gateWaiters  int64 = 1<<32 - gateWaiter // the waiting calls' count
	gateStarving int64 = 1 << 32            // one waiting call that starves, counted from here up
)

// starveAfter is how long a waiting call lets other calls take the gate ahead
// of it; then it starves, and only woken waiting calls take the gate until it
// has.
const starveAfter = time.Millisecond

// lock takes the gate, waiting while another call holds it unless stop
// closes or dl's deadline passes first, and reports whether it took it.
func (g *gate) lock(stop <-chan struct{}, dl *deadline) bool {
	if g.state.CompareAndSwap(0, gateLocked) {
		return true
	}
	return g.lockSlow(stop, dl)
}

// lockSlow takes the gate if it is free and no waiting call starves, else
// waits for it.
func (g *gate) lockSlow(stop <-chan struct{}, dl *deadline) bool {
	for {
		s := g.state.Load()
		if s&gateLocked == 0 && s < gateStarving {
			if g.state.CompareAndSwap(s, s|gateLocked) {
				return true
			}
		} else if g.state.CompareAndSwap(s, s+gateWaiter) {
			return g.wait(stop, dl.done())
		}
	}
}

// wait counts as a waiting call until, woken, it finds the gate free and
// takes it, or until stop or expired closes and it gives up. Finding the
// gate taken once it has waited longer than starveAfter, it starves.
func (g *gate) wait(stop, expired <-chan struct{}) bool {
	since := time.Now()
	var starving int64 // gateStarving once this call starves
	for {
		select {
		case <-g.baton:
		case <-stop:
			g.state.Add(-gateWaiter - starving)
			return false
		case <-expired:
			g.state.Add(-gateWaiter - starving)
			return false
		}
		for {
			s := g.state.Load()
			if s&gateLocked == 0 {
				if g.state.CompareAndSwap(s, (s|gateLocked)&^gateWoken-gateWaiter-starving) {
					return true
				}
				continue
			}
			starve := int64(0)
			if starving == 0 && time.Since(since) > starveAfter {
				starve = gateStarving
			}
			if g.state.CompareAndSwap(s, s&^gateWoken+starve) {
				starving += starve
				break
			}
		}
	}
}

// tryLock takes the gate if no call holds it, even ahead of waiting calls, as
// those may give up instead.
func (g *gate) tryLock() bool {
	for s := g.state.Load(); s&gateLocked == 0; s = g.state.Load() {
		if g.state.CompareAndSwap(s, s|gateLocked) {
			return true
		}
	}
	return false
}

// unlock frees the gate and wakes a waiting call to contend for it.
func (g *gate) unlock() {
	if s := g.state.Add(-gateLocked); s != 0 {
		g.wake(s)
	}
}

// wake wakes a waiting call unless the gate is taken again or a woken call
// has yet to contend; s is the state unlock left.
func (g *gate) wake(s int64) {
	for s&(gateLocked|gateWoken) == 0 && s&gateWaiters != 0 {
		if g.state.CompareAndSwap(s, s|gateWoken) {
			// No woken call means no wake-up in baton, so this never blocks.
			g.baton <- struct{}{}
			return
		}
		s = g.state.Load()
	}
}
