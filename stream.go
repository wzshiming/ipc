package ipc

import (
	"io"
	"os"
)

// Read copies the next bytes of the stream into p: the records the peer
// committed, in order, a long one continued by later Reads and a zero-length
// one skipped. It blocks like Next and, once the records are drained, reports
// the peer's close as io.EOF or its death as ErrPeerClosed. Past the read
// deadline it fails without consuming anything.
func (c *Conn) Read(p []byte) (int, error) {
	c.rmu.Lock()
	defer c.rmu.Unlock()
	if dl := c.rx.dl.Load(); dl != nil && dl.passed() {
		return 0, os.ErrDeadlineExceeded
	}
	if len(p) == 0 {
		return 0, nil
	}
	if err := c.hold(); err != nil {
		return 0, err
	}
	n := copy(p, c.rec)
	if c.rec = c.rec[n:]; len(c.rec) == 0 {
		c.Release()
	}
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

// WriteTo writes the stream to w without copying: the unread rest of a record
// Read holds, then every record's slice straight from the ring, each released
// after w.Write returns. It returns n and nil once the peer's close is
// drained, else Read's errors, w's error or io.ErrShortWrite, keeping the
// unwritten rest of the record for a later Read or WriteTo.
func (c *Conn) WriteTo(w io.Writer) (int64, error) {
	c.rmu.Lock()
	defer c.rmu.Unlock()
	if dl := c.rx.dl.Load(); dl != nil && dl.passed() {
		return 0, os.ErrDeadlineExceeded
	}
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
		if c.rec = c.rec[k:]; len(c.rec) == 0 {
			c.Release()
		} else if err == nil {
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
	c.wmu.Lock()
	defer c.wmu.Unlock()
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

// minRun is the least ReadFrom reserves; with less free it waits for room.
const minRun = 4 << 10

// ReadFrom sends what r reads without copying: each Read of r fills a record
// in place, reserved in the ring's free run up to MaxRecord, and exactly the
// bytes it returns are committed; with less than 4 KiB free it blocks for
// room like Reserve. It returns n and nil once r reports io.EOF, else r's
// error or Write's.
func (c *Conn) ReadFrom(r io.Reader) (int64, error) {
	c.wmu.Lock()
	defer c.wmu.Unlock()
	var n int64
	for {
		b, err := c.Reserve(max(c.room(), minRun))
		if err != nil {
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
