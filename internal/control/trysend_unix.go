//go:build unix

package control

import (
	"net"
	"os"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

// sender writes TrySend's frames on the raw socket, so that it tries before
// any clock runs and stops waiting once the bound has passed.
type sender struct {
	raw   syscall.RawConn
	try   func(fd uintptr) bool
	b     []byte
	err   error
	bound time.Duration // trySendWait unless a test raised it
	poll  [1]unix.PollFd
}

func (s *sender) init(c *net.UnixConn) {
	s.raw, _ = c.SyscallConn()
	s.try = s.write
	s.bound = trySendWait
}

// setBound replaces trySendWait for a test that load must not fail.
func (s *sender) setBound(d time.Duration) { s.bound = d }

func (s *sender) close() {}

func (s *sender) send(b []byte) error {
	s.b, s.err = b, nil
	if err := s.raw.Write(s.try); err != nil {
		return err
	}
	return s.err
}

// write writes s.b; while the socket refuses it, it waits for room until
// s.bound has passed since the first refusal, then tries a last time.
func (s *sender) write(fd uintptr) bool {
	var end time.Time
	for {
		n, err := unix.Write(int(fd), s.b)
		if n > 0 {
			s.b = s.b[n:]
		}
		switch {
		case len(s.b) == 0:
			return true
		case err == unix.EINTR:
			continue
		case err != nil && err != unix.EAGAIN:
			s.err = os.NewSyscallError("write", err)
			return true
		}
		now := time.Now()
		if end.IsZero() {
			end = now.Add(s.bound)
		} else if !now.Before(end) {
			s.err = ErrBusy
			return true
		}
		s.poll[0] = unix.PollFd{Fd: int32(fd), Events: unix.POLLOUT}
		unix.Poll(s.poll[:], int((end.Sub(now)+time.Millisecond-1)/time.Millisecond))
	}
}
