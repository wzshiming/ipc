package ipc

import (
	"context"
	"io"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// closeLinger bounds how long Close waits for room for the EOF marker.
const closeLinger = time.Second

// stream is a Lane presented as a net.Conn byte stream.
type stream struct {
	l *Lane

	rmu  sync.Mutex
	rd   deadline
	rec  []byte // unread rest of the record held from Next
	held bool
	eof  bool

	wmu sync.Mutex
	wd  deadline

	closed atomic.Bool
}

// NewStream returns a net.Conn that carries a byte stream over l: Write sends
// data as records of at most MaxRecord bytes and Read returns their bytes in
// order. Close sends an empty record that the peer's Read reports as io.EOF;
// it leaves the Conn and its other lanes open.
//
// A lane must be used either through the Lane API or through one Stream on
// each end, never both.
func NewStream(l *Lane) net.Conn {
	return &stream{l: l, rd: newDeadline(), wd: newDeadline()}
}

func (s *stream) Read(p []byte) (int, error) {
	s.rmu.Lock()
	defer s.rmu.Unlock()
	if err := s.rd.err(); err != nil {
		return 0, err
	}
	if s.eof {
		return 0, io.EOF
	}
	if len(p) == 0 {
		return 0, nil
	}
	if !s.held {
		rec, err := s.l.Next(&s.rd)
		if err != nil {
			return 0, err
		}
		if len(rec) == 0 {
			s.eof = true
			if err := s.l.Release(); err != nil {
				return 0, err
			}
			return 0, io.EOF
		}
		s.rec, s.held = rec, true
	}
	n := copy(p, s.rec)
	s.rec = s.rec[n:]
	if len(s.rec) == 0 {
		s.rec, s.held = nil, false
		if err := s.l.Release(); err != nil {
			return n, err
		}
	}
	return n, nil
}

func (s *stream) Write(p []byte) (int, error) {
	s.wmu.Lock()
	defer s.wmu.Unlock()
	if s.closed.Load() {
		return 0, ErrClosed
	}
	limit := s.l.c.MaxRecord()
	n := 0
	for n < len(p) {
		if err := s.wd.err(); err != nil {
			return n, err
		}
		k := min(len(p)-n, limit)
		b, err := s.l.Reserve(&s.wd, k)
		if err != nil {
			return n, err
		}
		copy(b, p[n:n+k])
		if err := s.l.Commit(); err != nil {
			return n, err
		}
		n += k
	}
	return n, nil
}

// Close wakes blocked calls, sends the EOF marker and releases a held record.
func (s *stream) Close() error {
	if !s.closed.CompareAndSwap(false, true) {
		return ErrClosed
	}
	s.rd.close()
	s.wd.close()

	s.wmu.Lock()
	err := s.sendEOF()
	s.wmu.Unlock()

	s.rmu.Lock()
	if s.held {
		s.rec, s.held = nil, false
		s.l.Release()
	}
	s.rmu.Unlock()
	return err
}

func (s *stream) sendEOF() error {
	_, err := s.l.TryReserve(0)
	if err == ErrWouldBlock {
		ctx, cancel := context.WithTimeout(context.Background(), closeLinger)
		_, err = s.l.Reserve(ctx, 0)
		cancel()
	}
	if err == nil {
		err = s.l.Commit()
	}
	if s.l.c.Err() != nil {
		// The peer learns of the end from the Conn itself.
		return nil
	}
	return err
}

func (s *stream) LocalAddr() net.Addr  { return s.l.c.LocalAddr() }
func (s *stream) RemoteAddr() net.Addr { return s.l.c.RemoteAddr() }

func (s *stream) SetDeadline(t time.Time) error {
	if err := s.rd.set(t); err != nil {
		return err
	}
	return s.wd.set(t)
}

func (s *stream) SetReadDeadline(t time.Time) error  { return s.rd.set(t) }
func (s *stream) SetWriteDeadline(t time.Time) error { return s.wd.set(t) }

// deadline is the context one direction of a stream blocks on: Done is
// closed when the deadline passes or the stream closes, and is replaced by
// an open channel when a later deadline is set.
type deadline struct {
	mu     sync.Mutex
	timer  *time.Timer
	cancel chan struct{}
	closed bool
}

func newDeadline() deadline { return deadline{cancel: make(chan struct{})} }

func (d *deadline) set(t time.Time) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return ErrClosed
	}
	d.stopTimer()
	expired := isClosed(d.cancel)
	if !t.IsZero() && !time.Now().Before(t) {
		if !expired {
			close(d.cancel)
		}
		return nil
	}
	if expired {
		d.cancel = make(chan struct{})
	}
	if !t.IsZero() {
		ch := d.cancel
		d.timer = time.AfterFunc(time.Until(t), func() { close(ch) })
	}
	return nil
}

// stopTimer stops a pending timer, or waits until a fired one closed cancel.
func (d *deadline) stopTimer() {
	if d.timer != nil && !d.timer.Stop() {
		<-d.cancel
	}
	d.timer = nil
}

func (d *deadline) close() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.stopTimer()
	d.closed = true
	if !isClosed(d.cancel) {
		close(d.cancel)
	}
}

// err returns ErrClosed or os.ErrDeadlineExceeded once a call must not
// proceed, else nil.
func (d *deadline) err() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	switch {
	case d.closed:
		return ErrClosed
	case isClosed(d.cancel):
		return os.ErrDeadlineExceeded
	}
	return nil
}

func (d *deadline) Deadline() (time.Time, bool) { return time.Time{}, false }
func (d *deadline) Value(any) any               { return nil }

func (d *deadline) Done() <-chan struct{} {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.cancel
}

// Err is consulted only after Done was seen closed, so it reports a timeout
// even if a later deadline was set in between.
func (d *deadline) Err() error {
	if err := d.err(); err != nil {
		return err
	}
	return os.ErrDeadlineExceeded
}

func isClosed(ch chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}
