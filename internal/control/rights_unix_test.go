//go:build darwin || linux

package control

import (
	"os"
	"testing"

	"golang.org/x/sys/unix"
)

func recvOneFD(t *testing.T, w *os.File) int {
	t.Helper()
	a, b := pair(t)
	if err := a.Send(7, []byte("fd"), []int{int(w.Fd())}); err != nil {
		t.Fatal(err)
	}
	typ, p, fds, err := b.Recv()
	if err != nil {
		t.Fatal(err)
	}
	if typ != 7 || string(p) != "fd" || len(fds) != 1 {
		t.Fatalf("typ=%d payload=%q fds=%v", typ, p, fds)
	}
	t.Cleanup(func() { unix.Close(fds[0]) })
	return fds[0]
}

func TestSendPassesPipeWriteEnd(t *testing.T) {
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	defer w.Close()

	fd := recvOneFD(t, w)
	if fd == int(w.Fd()) {
		t.Fatalf("received fd %d is the sent fd, want a new descriptor", fd)
	}
	if _, err := unix.Write(fd, []byte("hello")); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 5)
	if _, err := r.Read(buf); err != nil || string(buf) != "hello" {
		t.Fatalf("pipe read %q, %v", buf, err)
	}
}

func TestReceivedFDIsCloseOnExec(t *testing.T) {
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	defer w.Close()

	fd := recvOneFD(t, w)
	flags, err := unix.FcntlInt(uintptr(fd), unix.F_GETFD, 0)
	if err != nil {
		t.Fatal(err)
	}
	if flags&unix.FD_CLOEXEC == 0 {
		t.Fatalf("fd %d flags %#x lack FD_CLOEXEC", fd, flags)
	}
}

func TestFramesAfterFDFrameStayAligned(t *testing.T) {
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	defer w.Close()

	a, b := pair(t)
	fd := int(w.Fd())
	for i := range 3 {
		var fds []int
		if i == 1 {
			fds = []int{fd, fd}
		}
		if err := a.Send(uint8(i), []byte{byte(i), byte(i)}, fds); err != nil {
			t.Fatal(err)
		}
	}
	for i := range 3 {
		typ, p, fds, err := b.Recv()
		if err != nil {
			t.Fatal(err)
		}
		for _, f := range fds {
			unix.Close(f)
		}
		want := 0
		if i == 1 {
			want = 2
		}
		if typ != uint8(i) || len(p) != 2 || p[0] != byte(i) || len(fds) != want {
			t.Fatalf("frame %d: typ=%d payload=%v fds=%v", i, typ, p, fds)
		}
	}
}
