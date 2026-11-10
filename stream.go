package ipc

import "os"

// Read copies the next bytes of the stream into p: the records the peer
// committed, in order, a long one continued by later Reads and a zero-length
// one skipped. It blocks like Next and, once the records are drained, reports
// the peer's close as io.EOF or its death as ErrPeerClosed. Past the read
// deadline it fails without consuming anything.
func (c *Conn) Read(p []byte) (int, error) {
	c.rmu.Lock()
	defer c.rmu.Unlock()
	if c.rx.dl.expired.Load() {
		return 0, os.ErrDeadlineExceeded
	}
	if len(p) == 0 {
		return 0, nil
	}
	for len(c.rec) == 0 {
		rec, err := c.Next()
		if err != nil {
			return 0, err
		}
		if len(rec) == 0 {
			c.Release()
			continue
		}
		c.rec = rec
	}
	n := copy(p, c.rec)
	if c.rec = c.rec[n:]; len(c.rec) == 0 {
		c.Release()
	}
	return n, nil
}

// Write sends p as records of at most MaxRecord bytes, blocking for room
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
