package ipc

import (
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
	rec  []byte // unread rest of the record held from Next
	held bool
	eof  bool

	wmu sync.Mutex

	dmu    sync.Mutex // orders deadline changes with Close
	closed atomic.Bool
}

// NewStream returns a net.Conn that carries a byte stream over l: Write sends
// data as records of at most MaxRecord bytes and Read returns their bytes in
// order. Its deadlines are the lane's. Close sends an empty record that the
// peer's Read reports as io.EOF; it leaves the Conn and its other lanes open.
// After the peer's Conn closes, Read returns the bytes already sent, then
// io.EOF if the peer closed gracefully or ErrPeerClosed if it died.
//
// A lane must be used either through the Lane API or through one Stream on
// each end, never both.
func NewStream(l *Lane) net.Conn {
	return &stream{l: l}
}

// err reports a lane error, or net.ErrClosed once the stream is closed.
func (s *stream) err(err error) error {
	if s.closed.Load() {
		return net.ErrClosed
	}
	return err
}

func (s *stream) Read(p []byte) (int, error) {
	s.rmu.Lock()
	defer s.rmu.Unlock()
	if s.closed.Load() {
		return 0, net.ErrClosed
	}
	if s.l.rx.dl.expired.Load() {
		return 0, os.ErrDeadlineExceeded
	}
	if s.eof {
		return 0, io.EOF
	}
	if len(p) == 0 {
		return 0, nil
	}
	if !s.held {
		rec, err := s.l.Next()
		if err != nil {
			return 0, s.err(err)
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
		return 0, net.ErrClosed
	}
	limit := s.l.c.MaxRecord()
	n := 0
	for n < len(p) {
		k := min(len(p)-n, limit)
		b, err := s.l.Reserve(k)
		if err != nil {
			return n, s.err(err)
		}
		copy(b, p[n:n+k])
		if err := s.l.Commit(); err != nil {
			return n, s.err(err)
		}
		n += k
	}
	return n, nil
}

// Close wakes blocked calls, sends the EOF marker, releases a held record
// and clears the lane's deadlines.
func (s *stream) Close() error {
	s.dmu.Lock()
	if s.closed.Load() {
		s.dmu.Unlock()
		return net.ErrClosed
	}
	s.closed.Store(true)
	s.l.SetDeadline(time.Unix(1, 0))
	s.dmu.Unlock()

	s.wmu.Lock()
	err := s.sendEOF()
	s.wmu.Unlock()

	s.rmu.Lock()
	if s.held {
		s.rec, s.held = nil, false
		s.l.Release()
	}
	s.rmu.Unlock()
	s.l.SetDeadline(time.Time{})
	return err
}

func (s *stream) sendEOF() error {
	_, err := s.l.TryReserve(0)
	if err == ErrWouldBlock {
		s.l.SetWriteDeadline(time.Now().Add(closeLinger))
		_, err = s.l.Reserve(0)
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
	return s.setDeadline(s.l.SetDeadline, t)
}

func (s *stream) SetReadDeadline(t time.Time) error {
	return s.setDeadline(s.l.SetReadDeadline, t)
}

func (s *stream) SetWriteDeadline(t time.Time) error {
	return s.setDeadline(s.l.SetWriteDeadline, t)
}

// setDeadline sets a lane deadline unless Close has taken them over.
func (s *stream) setDeadline(set func(time.Time) error, t time.Time) error {
	s.dmu.Lock()
	defer s.dmu.Unlock()
	if s.closed.Load() {
		return net.ErrClosed
	}
	return set(t)
}
