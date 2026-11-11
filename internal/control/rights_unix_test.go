//go:build darwin || linux

package control

import (
	"net"
	"os"
	"runtime"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

// openFDs counts the descriptors this process has open.
func openFDs(t *testing.T) int {
	t.Helper()
	dir := "/dev/fd"
	if runtime.GOOS == "linux" {
		dir = "/proc/self/fd"
	}
	ents, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	return len(ents)
}

// expectOpenFDs fails unless the open descriptor count settles at want; it
// polls because an earlier test's goroutine may still be closing a socket.
func expectOpenFDs(t *testing.T, want int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		got := openFDs(t)
		if got == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d descriptors open, want %d", got, want)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// pipeFD returns a descriptor to attach to frames, open until the test ends.
func pipeFD(t *testing.T) int {
	t.Helper()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { r.Close(); w.Close() })
	return int(r.Fd())
}

func repeatFD(fd, n int) []int {
	fds := make([]int, n)
	for i := range fds {
		fds[i] = fd
	}
	return fds
}

// sendFrame writes one frame through c by hand, with fds attached to the
// frame's byte at offset at.
func sendFrame(t *testing.T, c *net.UnixConn, typ uint8, payload []byte, at int, fds []int) {
	t.Helper()
	f := append([]byte{typ, 0, 0, 0, 0}, payload...)
	putLength(f[1:headerSize], len(payload))
	if at > 0 {
		if _, err := c.Write(f[:at]); err != nil {
			t.Fatal(err)
		}
	}
	if _, _, err := c.WriteMsgUnix(f[at:at+1], unix.UnixRights(fds...), nil); err != nil {
		t.Fatalf("sending %d descriptors: %v", len(fds), err)
	}
	if at+1 < len(f) {
		if _, err := c.Write(f[at+1:]); err != nil {
			t.Fatal(err)
		}
	}
}

func TestRecvHandsFirstByteFDsToTheCallerToClose(t *testing.T) {
	fd := pipeFD(t)
	a, b := pair(t)
	before := openFDs(t)
	sendFrame(t, b.c, 7, []byte("fd"), 0, repeatFD(fd, 3))
	typ, p, fds, err := a.Recv()
	if err != nil || typ != 7 || string(p) != "fd" || len(fds) != 3 {
		t.Fatalf("typ=%d payload=%q fds=%v err=%v", typ, p, fds, err)
	}
	expectOpenFDs(t, before+3)
	CloseFDs(fds)
	expectOpenFDs(t, before)
}

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
