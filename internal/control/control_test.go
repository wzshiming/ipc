package control

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
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
	l, err := Listen(path)
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
