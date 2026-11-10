package ipc

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"math/rand/v2"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"golang.org/x/net/nettest"
)

// envCopyMiB sets the cross-process io.Copy volume in MiB (default 1024).
const envCopyMiB = "IPC_E2E_COPY_MIB"

// streamPair returns the listener-side and dialer-side Streams of lane 0.
func streamPair(t testing.TB, cfg Config) (ls, ds net.Conn) {
	lc, dc := pair(t, cfg, cfg)
	ls, ds = NewStream(lc.Lane(0)), NewStream(dc.Lane(0))
	t.Cleanup(func() {
		ls.Close()
		ds.Close()
	})
	return ls, ds
}

func makeStreamPipe() (c1, c2 net.Conn, stop func(), err error) {
	dir, err := os.MkdirTemp("", "ipc")
	if err != nil {
		return nil, nil, nil, err
	}
	path := filepath.Join(dir, "s")
	ln, err := Listen(path, Config{})
	if err != nil {
		os.RemoveAll(dir)
		return nil, nil, nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ch := make(chan *Conn, 1)
	go func() {
		c, _ := ln.Accept(ctx)
		ch <- c
	}()
	dc, err := Dial(ctx, path, Config{})
	lc := <-ch
	if err != nil || lc == nil {
		if dc != nil {
			dc.Close()
		}
		if lc != nil {
			lc.Close()
		}
		ln.Close()
		os.RemoveAll(dir)
		if err == nil {
			err = errors.New("Accept failed")
		}
		return nil, nil, nil, err
	}
	c1, c2 = NewStream(lc.Lane(0)), NewStream(dc.Lane(0))
	stop = func() {
		c1.Close()
		c2.Close()
		lc.Close()
		dc.Close()
		ln.Close()
		os.RemoveAll(dir)
	}
	return c1, c2, stop, nil
}

func TestStreamConformsToNetConn(t *testing.T) {
	nettest.TestConn(t, makeStreamPipe)
}

func TestStreamReadKeepsUnreadRecordAcrossReads(t *testing.T) {
	ls, ds := streamPair(t, Config{})
	if _, err := ls.Write([]byte("abcdef")); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 2)
	var got []byte
	for range 3 {
		n, err := ds.Read(buf)
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, buf[:n]...)
	}
	if string(got) != "abcdef" {
		t.Fatalf("three 2-byte Reads = %q; want abcdef", got)
	}
}

func TestStreamEmptyWriteSendsNothing(t *testing.T) {
	ls, ds := streamPair(t, Config{})
	if n, err := ls.Write(nil); n != 0 || err != nil {
		t.Fatalf("Write(nil) = %d, %v; want 0, nil", n, err)
	}
	ls.Write([]byte("x"))
	buf := make([]byte, 4)
	if n, err := ds.Read(buf); err != nil || string(buf[:n]) != "x" {
		t.Fatalf("Read after empty Write = %q, %v; want x", buf[:n], err)
	}
}

func TestStreamWriteSplitsIntoMaxRecords(t *testing.T) {
	ls, ds := streamPair(t, Config{RingSize: 64 << 10})
	want := make([]byte, 5*(64<<10)+123)
	rand.NewChaCha8([32]byte{1}).Read(want)
	go func() {
		ls.Write(want)
		ls.Close()
	}()
	got, err := io.ReadAll(ds)
	if err != nil || !bytes.Equal(got, want) {
		t.Fatalf("ReadAll = %d bytes, %v; want the %d bytes written", len(got), err, len(want))
	}
}

func TestStreamPeerReadsEOFAfterDrainingDataWrittenBeforeClose(t *testing.T) {
	ls, ds := streamPair(t, Config{})
	ls.Write([]byte("bye"))
	if err := ls.Close(); err != nil {
		t.Fatal("Close:", err)
	}
	buf := make([]byte, 8)
	n, err := ds.Read(buf)
	if err != nil || string(buf[:n]) != "bye" {
		t.Fatalf("Read = %q, %v; want bye", buf[:n], err)
	}
	for range 2 {
		if n, err := ds.Read(buf); n != 0 || err != io.EOF {
			t.Fatalf("Read after drain = %d, %v; want 0, io.EOF", n, err)
		}
	}
}

func TestStreamReadDrainsThenEOFAfterPeerClosesStreamAndConn(t *testing.T) {
	lc, dc := pair(t, Config{}, Config{})
	ls, ds := NewStream(lc.Lane(0)), NewStream(dc.Lane(0))
	want := make([]byte, 1<<20)
	rand.NewChaCha8([32]byte{2}).Read(want)
	if _, err := ls.Write(want); err != nil {
		t.Fatal(err)
	}
	ls.Close()
	lc.Close()
	<-dc.Done()
	got, err := io.ReadAll(ds)
	if err != nil || !bytes.Equal(got, want) {
		t.Fatalf("ReadAll after peer close = %d bytes, %v; want the %d bytes written", len(got), err, len(want))
	}
	if dc.unmapped.Load() {
		t.Fatal("segment unmapped before local Close")
	}
	ds.Close()
	dc.Close()
	if !dc.unmapped.Load() {
		t.Fatal("segment still mapped after local Close")
	}
}

func TestStreamReadDrainsThenErrPeerClosedAfterPeerCrash(t *testing.T) {
	lc, dc := pair(t, Config{}, Config{})
	ls, ds := NewStream(lc.Lane(0)), NewStream(dc.Lane(0))
	want := make([]byte, 1<<20)
	rand.NewChaCha8([32]byte{3}).Read(want)
	if _, err := ls.Write(want); err != nil {
		t.Fatal(err)
	}
	lc.link.Close() // vanish without the EOF marker or Bye
	<-dc.Done()
	got := make([]byte, len(want))
	if _, err := io.ReadFull(ds, got); err != nil || !bytes.Equal(got, want) {
		t.Fatalf("ReadFull after peer crash: %v; want the %d bytes written", err, len(want))
	}
	if _, err := ds.Read(got); !errors.Is(err, ErrPeerClosed) {
		t.Fatalf("Read after drain = %v; want ErrPeerClosed", err)
	}
}

func TestStreamWriteAfterCloseIsNetErrClosed(t *testing.T) {
	ls, _ := streamPair(t, Config{})
	ls.Close()
	if _, err := ls.Write([]byte("x")); err != net.ErrClosed {
		t.Fatalf("Write after Close = %v; want net.ErrClosed", err)
	}
	if _, err := ls.Read(make([]byte, 1)); err != net.ErrClosed {
		t.Fatalf("Read after Close = %v; want net.ErrClosed", err)
	}
}

func TestStreamCloseWakesBlockedRead(t *testing.T) {
	_, ds := streamPair(t, Config{Spin: -1})
	ch := make(chan error, 1)
	go func() {
		_, err := ds.Read(make([]byte, 8))
		ch <- err
	}()
	time.Sleep(20 * time.Millisecond)
	ds.Close()
	if err := waitErr(t, ch, time.Second); err != net.ErrClosed {
		t.Fatalf("blocked Read after Close = %v; want net.ErrClosed", err)
	}
}

func TestStreamReadDrainsThenEOFAfterPeerClosesConnWithoutMarker(t *testing.T) {
	lc, dc := pair(t, Config{}, Config{})
	ls, ds := NewStream(lc.Lane(0)), NewStream(dc.Lane(0))
	if _, err := ls.Write([]byte("bye")); err != nil {
		t.Fatal(err)
	}
	lc.Close()
	<-dc.Done()
	got, err := io.ReadAll(ds)
	if err != nil || string(got) != "bye" {
		t.Fatalf("ReadAll after peer Conn.Close = %q, %v; want bye, nil", got, err)
	}
}

func TestStreamCloseLeavesConnOpen(t *testing.T) {
	lc, dc := pair(t, Config{Lanes: 2}, Config{Lanes: 2})
	if err := NewStream(lc.Lane(0)).Close(); err != nil {
		t.Fatal(err)
	}
	if err := lc.Err(); err != nil {
		t.Fatalf("Conn.Err after Stream.Close = %v; want nil", err)
	}
	if _, err := lc.Lane(1).Write([]byte("x")); err != nil {
		t.Fatal(err)
	}
	if _, err := dc.Lane(1).Read(make([]byte, 1)); err != nil {
		t.Fatal(err)
	}
}

func TestStreamDeadlineErrorIsNetTimeout(t *testing.T) {
	_, ds := streamPair(t, Config{})
	ds.SetReadDeadline(time.Now().Add(20 * time.Millisecond))
	_, err := ds.Read(make([]byte, 8))
	var ne net.Error
	if !errors.As(err, &ne) || !ne.Timeout() {
		t.Fatalf("Read past deadline = %v; want a net.Error with Timeout()", err)
	}
}

func TestStreamAddrsAreConnAddrs(t *testing.T) {
	lc, _ := pair(t, Config{}, Config{})
	s := NewStream(lc.Lane(0))
	defer s.Close()
	if s.LocalAddr() != lc.LocalAddr() || s.RemoteAddr() != lc.RemoteAddr() {
		t.Fatalf("addrs = %v, %v; want %v, %v", s.LocalAddr(), s.RemoteAddr(), lc.LocalAddr(), lc.RemoteAddr())
	}
}

// fillReader returns len(p) bytes of whatever p holds, at no cost.
type fillReader struct{}

func (fillReader) Read(p []byte) (int, error) { return len(p), nil }

func TestStreamIOCopyThroughput(t *testing.T) {
	ls, ds := streamPair(t, Config{})
	volume := int64(4 << 30)
	if raceEnabled() {
		volume = 256 << 20
	}
	errc := make(chan error, 1)
	start := time.Now()
	go func() {
		_, err := io.Copy(ls, io.LimitReader(fillReader{}, volume))
		ls.Close()
		errc <- err
	}()
	n, err := io.Copy(io.Discard, ds)
	d := time.Since(start)
	if err != nil || n != volume {
		t.Fatalf("io.Copy = %d, %v; want %d", n, err, volume)
	}
	if err := <-errc; err != nil {
		t.Fatal(err)
	}
	rate := float64(volume) / d.Seconds()
	t.Logf("in-process io.Copy of %d MiB: %.2f GB/s (race=%v)", volume>>20, rate/1e9, raceEnabled())
	if !raceEnabled() && rate < 2e9 {
		t.Fatalf("throughput %.2f GB/s below 2 GB/s", rate/1e9)
	}
}

// runCopyChild echoes the stream on lane 0 until EOF.
func runCopyChild(c *Conn) error {
	s := NewStream(c.Lane(0))
	_, err := io.Copy(s, s)
	s.Close()
	return err
}

func copyVolume() int64 {
	mib := int64(1024)
	if v, err := strconv.ParseInt(os.Getenv(envCopyMiB), 10, 64); err == nil && v > 0 {
		mib = v
	}
	return mib << 20
}

func TestE2EStreamIOCopyEchoesRandomData(t *testing.T) {
	c, cmd := startPeer(t, "copy", Config{})
	s := NewStream(c.Lane(0))
	volume := copyVolume()
	start := time.Now()
	sent := make(chan []byte, 1)
	go func() {
		h := sha256.New()
		src := io.TeeReader(io.LimitReader(rand.NewChaCha8([32]byte{7}), volume), h)
		if _, err := io.Copy(s, src); err != nil {
			t.Error("Write:", err)
		}
		sent <- h.Sum(nil)
	}()
	h := sha256.New()
	n, err := io.Copy(h, io.LimitReader(s, volume))
	if err != nil || n != volume {
		t.Fatalf("echo read %d bytes, %v; want %d", n, err, volume)
	}
	if want := <-sent; !bytes.Equal(h.Sum(nil), want) {
		t.Fatal("echoed data differs from data sent")
	}
	d := time.Since(start)
	if err := s.Close(); err != nil {
		t.Fatal("Close:", err)
	}
	if err := cmd.Wait(); err != nil {
		t.Fatal("child:", err)
	}
	t.Logf("echoed %d MiB through a child io.Copy in %v: %.2f GB/s each way", volume>>20, d, float64(volume)/d.Seconds()/1e9)
}
