//go:build windows

package control

import (
	"net"
	"syscall"
	"time"

	"golang.org/x/sys/windows"
)

// sender writes TrySend's frames with an overlapped WSASend of its own, so
// that its wait starts once the send is issued and ends in a cancel.
type sender struct {
	raw   syscall.RawConn
	try   func(fd uintptr) bool
	ev    windows.Handle
	evErr error
	ov    windows.Overlapped
	buf   windows.WSABuf
	b     []byte
	err   error
}

func (s *sender) init(c *net.UnixConn) {
	s.raw, _ = c.SyscallConn()
	s.ev, s.evErr = windows.CreateEvent(nil, 1, 0, nil)
	s.try = s.write
}

// close runs after the socket closed, which waits for a write in progress.
func (s *sender) close() {
	if s.evErr == nil {
		windows.CloseHandle(s.ev)
	}
}

func (s *sender) send(b []byte) error {
	if s.evErr != nil {
		return s.evErr
	}
	s.b, s.err = b, nil
	if err := s.raw.Write(s.try); err != nil {
		return err
	}
	return s.err
}

// write sends s.b and cancels the send once it has waited trySendWait.
func (s *sender) write(fd uintptr) bool {
	h := windows.Handle(fd)
	if s.err = windows.ResetEvent(s.ev); s.err != nil {
		return true
	}
	// The event's low bit keeps the completion off the runtime's port.
	s.ov = windows.Overlapped{HEvent: s.ev | 1}
	s.buf = windows.WSABuf{Len: uint32(len(s.b)), Buf: &s.b[0]}
	err := windows.WSASend(h, &s.buf, 1, nil, 0, &s.ov, nil)
	if err == windows.ERROR_IO_PENDING {
		if !s.wait() {
			windows.CancelIoEx(h, &s.ov)
			windows.WaitForSingleObject(s.ev, windows.INFINITE)
		}
		var n, flags uint32
		if err = windows.WSAGetOverlappedResult(h, &s.ov, &n, false, &flags); err == nil && int(n) < len(s.b) {
			err = ErrBusy
		}
	}
	if err == windows.ERROR_OPERATION_ABORTED {
		err = ErrBusy
	}
	s.err = err
	return true
}

// wait reports whether the send completed within trySendWait by the clock.
func (s *sender) wait() bool {
	end := time.Now().Add(trySendWait)
	for {
		ms := (time.Until(end) + time.Millisecond - 1) / time.Millisecond
		if ev, _ := windows.WaitForSingleObject(s.ev, uint32(max(ms, 0))); ev == windows.WAIT_OBJECT_0 {
			return true
		}
		if !time.Now().Before(end) {
			return false
		}
	}
}
