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

func TestRecvClosesFDsOnOtherBytesAndDeliversTheFrame(t *testing.T) {
	fd := pipeFD(t)
	for _, tc := range []struct {
		name string
		at   int
	}{
		{"second header byte", 1},
		{"last header byte", headerSize - 1},
		{"first payload byte", headerSize},
		{"later payload byte", headerSize + 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a, b := pair(t)
			before := openFDs(t)
			sendFrame(t, b.c, 9, []byte("payload"), tc.at, repeatFD(fd, 4))
			typ, p, fds, err := a.Recv()
			if err != nil || typ != 9 || string(p) != "payload" || len(fds) != 0 {
				t.Fatalf("typ=%d payload=%q fds=%v err=%v", typ, p, fds, err)
			}
			expectOpenFDs(t, before)
		})
	}
}

func TestRecvOfMoreThanMaxFDsOnTheFirstByteFailsWithoutLeak(t *testing.T) {
	fd := pipeFD(t)
	a, b := pair(t)
	before := openFDs(t)
	sendFrame(t, b.c, 1, []byte("hello"), 0, repeatFD(fd, 64))
	if _, _, fds, err := a.Recv(); err == nil {
		t.Fatalf("Recv of 64 descriptors succeeded with %d", len(fds))
	}
	expectOpenFDs(t, before)
}

// rightsLimit finds the most descriptors one message can carry on this OS.
func rightsLimit(t *testing.T, fd int) int {
	t.Helper()
	fits := func(n int) bool {
		sp, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM, 0)
		if err != nil {
			t.Fatal(err)
		}
		defer unix.Close(sp[0])
		defer unix.Close(sp[1])
		return unix.Sendmsg(sp[0], []byte{0}, unix.UnixRights(repeatFD(fd, n)...), nil, 0) == nil
	}
	lo, hi := MaxFDs, 4096
	if !fits(lo) || fits(hi) {
		t.Fatalf("the limit is not between %d and %d descriptors", lo, hi)
	}
	for hi-lo > 1 {
		if mid := (lo + hi) / 2; fits(mid) {
			lo = mid
		} else {
			hi = mid
		}
	}
	return lo
}

func TestRecvOfTheLargestRightsMessageLeavesNoDescriptor(t *testing.T) {
	fd := pipeFD(t)
	n := rightsLimit(t, fd)
	t.Logf("one message carries at most %d descriptors on %s", n, runtime.GOOS)
	t.Run("first byte", func(t *testing.T) {
		a, b := pair(t)
		before := openFDs(t)
		sendFrame(t, b.c, 1, []byte("hello"), 0, repeatFD(fd, n))
		if _, _, fds, err := a.Recv(); err == nil {
			t.Fatalf("Recv of %d descriptors succeeded with %d", n, len(fds))
		}
		expectOpenFDs(t, before)
	})
	t.Run("second byte", func(t *testing.T) {
		a, b := pair(t)
		before := openFDs(t)
		sendFrame(t, b.c, 1, []byte("hello"), 1, repeatFD(fd, n))
		typ, p, fds, err := a.Recv()
		if err != nil || typ != 1 || string(p) != "hello" || len(fds) != 0 {
			t.Fatalf("typ=%d payload=%q fds=%v err=%v", typ, p, fds, err)
		}
		expectOpenFDs(t, before)
	})
}

func TestRecvDeliversFiftyFramesInOrderClosingSecondByteFDs(t *testing.T) {
	fd := pipeFD(t)
	a, b := pair(t)
	before := openFDs(t)
	for i := range 50 {
		sendFrame(t, b.c, 4, []byte{byte(i)}, 1, repeatFD(fd, 8))
	}
	for i := range 50 {
		typ, p, fds, err := a.Recv()
		if err != nil || typ != 4 || len(p) != 1 || p[0] != byte(i) || len(fds) != 0 {
			t.Fatalf("frame %d: typ=%d payload=%v fds=%v err=%v", i, typ, p, fds, err)
		}
	}
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
