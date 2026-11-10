package ipc

import (
	"bytes"
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

func makePipe() (c1, c2 net.Conn, stop func(), err error) {
	dir, err := os.MkdirTemp("", "ipc")
	if err != nil {
		return nil, nil, nil, err
	}
	path := filepath.Join(dir, "s")
	ln, err := Listen(path)
	if err != nil {
		os.RemoveAll(dir)
		return nil, nil, nil, err
	}
	ln.SetDeadline(time.Now().Add(10 * time.Second))
	ch := make(chan net.Conn, 1)
	go func() {
		c, _ := ln.Accept()
		ch <- c
	}()
	dc, err := (&Dialer{Timeout: 10 * time.Second}).Dial(path)
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
	stop = func() {
		lc.Close()
		dc.Close()
		ln.Close()
		os.RemoveAll(dir)
	}
	return lc, dc, stop, nil
}

func TestConnConformsToNetConn(t *testing.T) {
	nettest.TestConn(t, makePipe)
}

func TestReadKeepsUnreadRecordAcrossReads(t *testing.T) {
	lc, dc := pair(t, ListenConfig{}, Dialer{})
	if _, err := lc.Write([]byte("abcdef")); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 2)
	var got []byte
	for range 3 {
		n, err := dc.Read(buf)
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, buf[:n]...)
	}
	if string(got) != "abcdef" {
		t.Fatalf("three 2-byte Reads = %q; want abcdef", got)
	}
}

func TestEmptyWriteSendsNothing(t *testing.T) {
	lc, dc := pair(t, ListenConfig{}, Dialer{})
	if n, err := lc.Write(nil); n != 0 || err != nil {
		t.Fatalf("Write(nil) = %d, %v; want 0, nil", n, err)
	}
	if _, err := dc.TryNext(); err != ErrWouldBlock {
		t.Fatalf("TryNext after Write(nil) = %v; want ErrWouldBlock", err)
	}
}

func TestReadSkipsZeroLengthRecords(t *testing.T) {
	lc, dc := pair(t, ListenConfig{}, Dialer{})
	for range 2 {
		if _, err := lc.Reserve(0); err != nil {
			t.Fatal(err)
		}
		if err := lc.Commit(); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := lc.Write([]byte("x")); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 4)
	if n, err := dc.Read(buf); err != nil || string(buf[:n]) != "x" {
		t.Fatalf("Read over zero-length records = %q, %v; want x", buf[:n], err)
	}
}

func TestWriteSplitsIntoMaxRecords(t *testing.T) {
	lc, dc := pair(t, ListenConfig{RingSize: 64 << 10}, Dialer{RingSize: 64 << 10})
	want := make([]byte, 5*(64<<10)+123)
	rand.NewChaCha8([32]byte{1}).Read(want)
	go func() {
		lc.Write(want)
		lc.Close()
	}()
	var got []byte
	records := 0
	for {
		rec, err := dc.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if len(rec) > dc.MaxRecord() {
			t.Fatalf("record of %d bytes exceeds MaxRecord %d", len(rec), dc.MaxRecord())
		}
		records++
		got = append(got, rec...)
		dc.Release()
	}
	if !bytes.Equal(got, want) || records <= 1 {
		t.Fatalf("%d records carrying %d bytes; want the %d bytes written in several records", records, len(got), len(want))
	}
}

func TestReadReassemblesSplitWriteInOrder(t *testing.T) {
	lc, dc := pair(t, ListenConfig{RingSize: 64 << 10}, Dialer{RingSize: 64 << 10})
	want := make([]byte, 5*(64<<10)+123)
	rand.NewChaCha8([32]byte{4}).Read(want)
	go func() {
		lc.Write(want)
		lc.Close()
	}()
	got, err := io.ReadAll(dc)
	if err != nil || !bytes.Equal(got, want) {
		t.Fatalf("ReadAll = %d bytes, %v; want the %d bytes written", len(got), err, len(want))
	}
}

func TestReadDrainsThenEOFAfterPeerClose(t *testing.T) {
	lc, dc := pair(t, ListenConfig{}, Dialer{})
	want := make([]byte, 1<<20)
	rand.NewChaCha8([32]byte{2}).Read(want)
	if _, err := lc.Write(want); err != nil {
		t.Fatal(err)
	}
	lc.Close()
	<-dc.Done()
	got, err := io.ReadAll(dc)
	if err != nil || !bytes.Equal(got, want) {
		t.Fatalf("ReadAll after peer close = %d bytes, %v; want the %d bytes written", len(got), err, len(want))
	}
	if n, err := dc.Read(make([]byte, 8)); n != 0 || err != io.EOF {
		t.Fatalf("Read after drain = %d, %v; want 0, io.EOF", n, err)
	}
	if dc.unmapped.Load() {
		t.Fatal("segment unmapped before local Close")
	}
	dc.Close()
	if !dc.unmapped.Load() {
		t.Fatal("segment still mapped after local Close")
	}
}

func TestReadDrainsThenErrPeerClosedAfterPeerCrash(t *testing.T) {
	lc, dc := pair(t, ListenConfig{}, Dialer{})
	want := make([]byte, 1<<20)
	rand.NewChaCha8([32]byte{3}).Read(want)
	if _, err := lc.Write(want); err != nil {
		t.Fatal(err)
	}
	lc.link.Close() // vanish without Bye
	<-dc.Done()
	got := make([]byte, len(want))
	if _, err := io.ReadFull(dc, got); err != nil || !bytes.Equal(got, want) {
		t.Fatalf("ReadFull after peer crash: %v; want the %d bytes written", err, len(want))
	}
	if _, err := dc.Read(got); !errors.Is(err, ErrPeerClosed) {
		t.Fatalf("Read after drain = %v; want ErrPeerClosed", err)
	}
}

func TestReadAndWriteAfterCloseAreNetErrClosed(t *testing.T) {
	lc, _ := pair(t, ListenConfig{}, Dialer{})
	lc.Close()
	if _, err := lc.Write([]byte("x")); err != net.ErrClosed {
		t.Fatalf("Write after Close = %v; want net.ErrClosed", err)
	}
	if _, err := lc.Read(make([]byte, 1)); err != net.ErrClosed {
		t.Fatalf("Read after Close = %v; want net.ErrClosed", err)
	}
}

func TestReadDeadlineErrorIsNetTimeout(t *testing.T) {
	_, dc := pair(t, ListenConfig{}, Dialer{})
	dc.SetReadDeadline(time.Now().Add(20 * time.Millisecond))
	_, err := dc.Read(make([]byte, 8))
	var ne net.Error
	if !errors.As(err, &ne) || !ne.Timeout() {
		t.Fatalf("Read past deadline = %v; want a net.Error with Timeout()", err)
	}
}

func TestReadAfterExpiredDeadlineKeepsHeldRecord(t *testing.T) {
	lc, dc := pair(t, ListenConfig{}, Dialer{})
	if _, err := lc.Write([]byte("abcdef")); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 3)
	if n, err := dc.Read(buf); err != nil || string(buf[:n]) != "abc" {
		t.Fatalf("first Read = %q, %v; want abc", buf[:n], err)
	}
	dc.SetReadDeadline(time.Now().Add(-time.Second))
	if n, err := dc.Read(buf); n != 0 || !isTimeout(err) {
		t.Fatalf("Read past deadline = %d, %v; want 0, os.ErrDeadlineExceeded", n, err)
	}
	dc.SetReadDeadline(time.Time{})
	if n, err := dc.Read(buf); err != nil || string(buf[:n]) != "def" {
		t.Fatalf("Read after clearing the deadline = %q, %v; want def", buf[:n], err)
	}
}

// fillReader returns len(p) bytes of whatever p holds, at no cost.
type fillReader struct{}

func (fillReader) Read(p []byte) (int, error) { return len(p), nil }

func TestIOCopyThroughput(t *testing.T) {
	lc, dc := pair(t, ListenConfig{}, Dialer{})
	volume := int64(4 << 30)
	if raceEnabled() {
		volume = 256 << 20
	}
	errc := make(chan error, 1)
	start := time.Now()
	go func() {
		_, err := io.Copy(lc, io.LimitReader(fillReader{}, volume))
		lc.Close()
		errc <- err
	}()
	n, err := io.Copy(io.Discard, dc)
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

// runCopyChild echoes copyVolume bytes, then closes so that the parent
// reads io.EOF.
func runCopyChild(c *Conn) error {
	_, err := io.Copy(c, io.LimitReader(c, copyVolume()))
	return err
}

func copyVolume() int64 {
	mib := int64(1024)
	if v, err := strconv.ParseInt(os.Getenv(envCopyMiB), 10, 64); err == nil && v > 0 {
		mib = v
	}
	return mib << 20
}

func TestE2EIOCopyEchoesRandomDataThenEOF(t *testing.T) {
	conns, cmd := startPeer(t, "copy", ListenConfig{}, 1)
	c := conns[0]
	volume := copyVolume()
	start := time.Now()
	sent := make(chan []byte, 1)
	go func() {
		h := sha256.New()
		src := io.TeeReader(io.LimitReader(rand.NewChaCha8([32]byte{7}), volume), h)
		if _, err := io.Copy(c, src); err != nil {
			t.Error("Write:", err)
		}
		sent <- h.Sum(nil)
	}()
	h := sha256.New()
	n, err := io.Copy(h, io.LimitReader(c, volume))
	if err != nil || n != volume {
		t.Fatalf("echo read %d bytes, %v; want %d", n, err, volume)
	}
	if want := <-sent; !bytes.Equal(h.Sum(nil), want) {
		t.Fatal("echoed data differs from data sent")
	}
	d := time.Since(start)
	if _, err := c.Read(make([]byte, 1)); err != io.EOF {
		t.Fatalf("Read after the child's Close = %v; want io.EOF", err)
	}
	if err := cmd.Wait(); err != nil {
		t.Fatal("child:", err)
	}
	t.Logf("echoed %d MiB through a child io.Copy in %v: %.2f GB/s each way", volume>>20, d, float64(volume)/d.Seconds()/1e9)
}
