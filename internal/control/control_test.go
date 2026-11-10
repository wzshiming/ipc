package control

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// sockPath returns a short socket path; t.TempDir() exceeds sun_path on macOS.
func sockPath(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "ipc")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	return filepath.Join(dir, "s.sock")
}

func listen(t *testing.T, path string) *Listener {
	t.Helper()
	l, err := Listen(t.Context(), path)
	if errors.Is(err, ErrUnsupported) {
		t.Skipf("AF_UNIX unsupported: %v", err)
	}
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	return l
}

// pair returns the accepted and the dialed end of one link.
func pair(t *testing.T) (*Link, *Link) {
	t.Helper()
	path := sockPath(t)
	l := listen(t, path)
	type res struct {
		c   *Link
		err error
	}
	ch := make(chan res, 1)
	go func() {
		c, err := l.Accept()
		ch <- res{c, err}
	}()
	d, err := Dial(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	r := <-ch
	if r.err != nil {
		d.Close()
		t.Fatal(r.err)
	}
	t.Cleanup(func() { d.Close(); r.c.Close() })
	return r.c, d
}

func TestRoundTripEmptyAndMaxFrames(t *testing.T) {
	a, b := pair(t)
	big := make([]byte, MaxPayload)
	for i := range big {
		big[i] = byte(i * 7)
	}
	errc := make(chan error, 1)
	go func() {
		if err := a.Send(1, nil, nil); err != nil {
			errc <- err
			return
		}
		errc <- a.Send(2, big, nil)
	}()

	typ, p, fds, err := b.Recv()
	if err != nil || typ != 1 || len(p) != 0 || len(fds) != 0 {
		t.Fatalf("empty frame: typ=%d len=%d fds=%v err=%v", typ, len(p), fds, err)
	}
	typ, p, _, err = b.Recv()
	if err != nil || typ != 2 || !bytes.Equal(p, big) {
		t.Fatalf("64 KiB frame: typ=%d len=%d err=%v", typ, len(p), err)
	}
	if err := <-errc; err != nil {
		t.Fatal(err)
	}
}

func TestRoundTripManySmallFramesInOrder(t *testing.T) {
	a, b := pair(t)
	const n = 100000
	errc := make(chan error, 1)
	go func() {
		var buf [8]byte
		for i := range n {
			binary.LittleEndian.PutUint32(buf[:], uint32(i))
			binary.LittleEndian.PutUint32(buf[4:], ^uint32(i))
			if err := a.Send(uint8(i), buf[:i%9], nil); err != nil {
				errc <- err
				return
			}
		}
		errc <- nil
	}()
	var want [8]byte
	for i := range n {
		typ, p, _, err := b.Recv()
		if err != nil {
			t.Fatalf("frame %d: %v", i, err)
		}
		binary.LittleEndian.PutUint32(want[:], uint32(i))
		binary.LittleEndian.PutUint32(want[4:], ^uint32(i))
		if typ != uint8(i) || !bytes.Equal(p, want[:i%9]) {
			t.Fatalf("frame %d: typ=%d payload=%x", i, typ, p)
		}
	}
	if err := <-errc; err != nil {
		t.Fatal(err)
	}
}

func TestConcurrentSendsDoNotInterleave(t *testing.T) {
	a, b := pair(t)
	const senders, perSender = 8, 10000
	errc := make(chan error, senders)
	for id := range senders {
		go func() {
			buf := make([]byte, 8+64)
			for seq := range perSender {
				binary.LittleEndian.PutUint32(buf, uint32(id))
				binary.LittleEndian.PutUint32(buf[4:], uint32(seq))
				n := 8 + seq%64
				for i := 8; i < n; i++ {
					buf[i] = byte(id)
				}
				if err := a.Send(uint8(id), buf[:n], nil); err != nil {
					errc <- err
					return
				}
			}
			errc <- nil
		}()
	}
	var next [senders]int
	for i := range senders * perSender {
		typ, p, _, err := b.Recv()
		if err != nil {
			t.Fatalf("frame %d: %v", i, err)
		}
		if len(p) < 8 {
			t.Fatalf("frame %d: short payload %x", i, p)
		}
		id := binary.LittleEndian.Uint32(p)
		seq := int(binary.LittleEndian.Uint32(p[4:]))
		if id >= senders || typ != uint8(id) || seq != next[id] || len(p) != 8+seq%64 {
			t.Fatalf("frame %d: typ=%d id=%d seq=%d len=%d", i, typ, id, seq, len(p))
		}
		for _, c := range p[8:] {
			if c != byte(id) {
				t.Fatalf("frame %d: sender %d payload corrupted: %x", i, id, p)
			}
		}
		next[id]++
	}
	for range senders {
		if err := <-errc; err != nil {
			t.Fatal(err)
		}
	}
}

func TestSendRejectsOversizePayload(t *testing.T) {
	a, _ := pair(t)
	err := a.Send(1, make([]byte, MaxPayload+1), nil)
	if !errors.Is(err, ErrPayloadTooLarge) {
		t.Fatalf("got %v, want ErrPayloadTooLarge", err)
	}
}

func TestSendRejectsTooManyFDs(t *testing.T) {
	a, _ := pair(t)
	err := a.Send(1, nil, make([]int, MaxFDs+1))
	if !errors.Is(err, ErrTooManyFDs) {
		t.Fatalf("got %v, want ErrTooManyFDs", err)
	}
}

func TestRecvReturnsEOFWhenPeerCloses(t *testing.T) {
	a, b := pair(t)
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := b.Recv(); err != io.EOF {
		t.Fatalf("got %v, want io.EOF", err)
	}
}

func TestDialMissingPathFailsFast(t *testing.T) {
	start := time.Now()
	_, err := Dial(t.Context(), sockPath(t))
	if err == nil {
		t.Fatal("dial to missing path succeeded")
	}
	if d := time.Since(start); d >= time.Second {
		t.Fatalf("dial took %v", d)
	}
}

func TestRecvReadDeadlineIsTimeout(t *testing.T) {
	_, b := pair(t)
	if err := b.SetReadDeadline(time.Now().Add(50 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := b.Recv(); !os.IsTimeout(err) {
		t.Fatalf("got %v, want a timeout error", err)
	}
}

func TestSendWriteDeadlineInPastIsTimeout(t *testing.T) {
	a, _ := pair(t)
	if err := a.SetWriteDeadline(time.Now().Add(-time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := a.Send(1, nil, nil); !os.IsTimeout(err) {
		t.Fatalf("got %v, want a timeout error", err)
	}
	if err := a.SetDeadline(time.Time{}); err != nil {
		t.Fatal(err)
	}
}

func TestCloseWakesBlockedRecvAndIsIdempotent(t *testing.T) {
	_, b := pair(t)
	errc := make(chan error, 1)
	go func() {
		_, _, _, err := b.Recv()
		errc <- err
	}()
	time.Sleep(50 * time.Millisecond)
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-errc:
		if !errors.Is(err, ErrClosed) && err != io.EOF {
			t.Fatalf("got %v, want ErrClosed or io.EOF", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not wake Recv")
	}
	if err := b.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	if err := b.Send(1, nil, nil); !errors.Is(err, ErrClosed) {
		t.Fatalf("Send after Close: got %v, want ErrClosed", err)
	}
	if _, _, _, err := b.Recv(); !errors.Is(err, ErrClosed) {
		t.Fatalf("Recv after Close: got %v, want ErrClosed", err)
	}
}

func TestListenerCloseUnblocksAcceptAndRemovesFile(t *testing.T) {
	path := sockPath(t)
	l := listen(t, path)
	errc := make(chan error, 1)
	go func() {
		_, err := l.Accept()
		errc <- err
	}()
	time.Sleep(50 * time.Millisecond)
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	if err := <-errc; err != ErrClosed {
		t.Fatalf("Accept: got %v, want ErrClosed", err)
	}
	if _, err := l.Accept(); err != ErrClosed {
		t.Fatalf("Accept after Close: got %v, want ErrClosed", err)
	}
	if _, err := os.Lstat(path); !os.IsNotExist(err) {
		t.Fatalf("socket file still present: %v", err)
	}
}

func TestListenerDeadlineTimesOutAccept(t *testing.T) {
	l := listen(t, sockPath(t))
	if err := l.SetDeadline(time.Now().Add(20 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	if _, err := l.Accept(); !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("Accept past deadline: got %v, want os.ErrDeadlineExceeded", err)
	}
}

func TestListenWithDoneCtxFails(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if l, err := Listen(ctx, sockPath(t)); !errors.Is(err, context.Canceled) {
		if l != nil {
			l.Close()
		}
		t.Fatalf("Listen with cancelled ctx: got %v, want context.Canceled", err)
	}
}

// staleSocket leaves a socket file at path with nobody listening on it.
func staleSocket(t *testing.T, path string) {
	t.Helper()
	ul, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		t.Skipf("AF_UNIX unavailable: %v", err)
	}
	ul.SetUnlinkOnClose(false)
	if err := ul.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(path); err != nil {
		t.Fatalf("stale socket file missing: %v", err)
	}
}

func TestDialStalePathFailsFast(t *testing.T) {
	path := sockPath(t)
	staleSocket(t, path)
	start := time.Now()
	if _, err := Dial(t.Context(), path); err == nil {
		t.Fatal("dial to stale path succeeded")
	}
	if d := time.Since(start); d >= time.Second {
		t.Fatalf("dial took %v", d)
	}
}

func TestListenReplacesStaleSocketFile(t *testing.T) {
	path := sockPath(t)
	staleSocket(t, path)
	l := listen(t, path)
	go func() {
		if c, err := Dial(t.Context(), path); err == nil {
			c.Close()
		}
	}()
	c, err := l.Accept()
	if err != nil {
		t.Fatal(err)
	}
	c.Close()
}

func TestListenOnLivePathIsAddrInUse(t *testing.T) {
	path := sockPath(t)
	listen(t, path)
	l2, err := Listen(t.Context(), path)
	if err == nil {
		l2.Close()
		t.Fatal("second Listen on a live path succeeded")
	}
	if _, err := os.Lstat(path); err != nil {
		t.Fatalf("live socket file removed: %v", err)
	}
	c, err := Dial(t.Context(), path)
	if err != nil {
		t.Fatalf("live listener no longer reachable: %v", err)
	}
	c.Close()
}

func TestListenDoesNotRemoveRegularFile(t *testing.T) {
	path := sockPath(t)
	if err := os.WriteFile(path, []byte("keep"), 0o600); err != nil {
		t.Fatal(err)
	}
	if l, err := Listen(t.Context(), path); err == nil {
		l.Close()
		t.Fatal("Listen over a regular file succeeded")
	}
	if b, err := os.ReadFile(path); err != nil || string(b) != "keep" {
		t.Fatalf("regular file clobbered: %q, %v", b, err)
	}
}
