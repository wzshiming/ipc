package ipc

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"testing/iotest"
	"time"
	"unsafe"

	"golang.org/x/net/nettest"

	"github.com/wzshiming/ipc/internal/ring"
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
}

// within reports whether p lies inside the segment c maps.
func within(c *Conn, p []byte) bool {
	seg := c.seg.Bytes()
	lo := uintptr(unsafe.Pointer(unsafe.SliceData(seg)))
	a := uintptr(unsafe.Pointer(unsafe.SliceData(p)))
	return len(seg) > 0 && a >= lo && a+uintptr(len(p)) <= lo+uintptr(len(seg))
}

// chunkReader serves r in reads of 1 to max bytes.
type chunkReader struct {
	r   io.Reader
	max int
	rng *rand.Rand
}

func (c *chunkReader) Read(p []byte) (int, error) {
	return c.r.Read(p[:min(len(p), 1+c.rng.IntN(c.max))])
}

// zeroFirst returns (0, nil) once, then reads from r.
type zeroFirst struct {
	r    io.Reader
	done bool
}

func (z *zeroFirst) Read(p []byte) (int, error) {
	if !z.done {
		z.done = true
		return 0, nil
	}
	return z.r.Read(p)
}

// capReader returns at most capReader bytes per Read, endlessly.
type capReader int

func (n capReader) Read(p []byte) (int, error) { return min(len(p), int(n)), nil }

var errSource = errors.New("source failed")

// errReader returns its data, then err along with the last of it.
type errReader struct {
	data []byte
	err  error
}

func (r *errReader) Read(p []byte) (int, error) {
	n := copy(p, r.data)
	if r.data = r.data[n:]; len(r.data) == 0 {
		return n, r.err
	}
	return n, nil
}

// probeReader returns one scripted length per Read, recording what each
// Read returned and how many slices lay outside c's segment.
type probeReader struct {
	c        *Conn
	lens     []int
	returned []int
	outside  int
}

func (r *probeReader) Read(p []byte) (int, error) {
	if len(r.lens) == 0 {
		return 0, io.EOF
	}
	if !within(r.c, p) {
		r.outside++
	}
	n := min(len(p), r.lens[0])
	r.lens = r.lens[1:]
	r.returned = append(r.returned, n)
	return n, nil
}

// stallReader fills the first slice it is handed, then on its second Read
// closes stalled and waits for resume before returning 0, nil, which sends
// ReadFrom back to reserving with nothing committed since; later Reads come
// from rest.
type stallReader struct {
	stalled, resume chan struct{}
	rest            io.Reader
	reads           int
}

func (r *stallReader) Read(p []byte) (int, error) {
	r.reads++
	switch r.reads {
	case 1:
		return len(p), nil
	case 2:
		close(r.stalled)
		<-r.resume
		return 0, nil
	}
	return r.rest.Read(p)
}

// probeWriter takes every slice whole, recording its length and whether it
// lay outside c's segment.
type probeWriter struct {
	c       *Conn
	lens    []int
	outside int
}

func (w *probeWriter) Write(p []byte) (int, error) {
	if !within(w.c, p) {
		w.outside++
	}
	w.lens = append(w.lens, len(p))
	return len(p), nil
}

// limitWriter takes at most n bytes per Write and returns err with them.
type limitWriter struct {
	n   int
	err error
	got []byte
}

func (w *limitWriter) Write(p []byte) (int, error) {
	k := min(len(p), w.n)
	w.got = append(w.got, p[:k]...)
	return k, w.err
}

// writerFunc is an io.Writer that calls itself.
type writerFunc func(p []byte) (int, error)

func (f writerFunc) Write(p []byte) (int, error) { return f(p) }

// stallWriter closes stalled on its first Write and waits for resume, then
// takes at most max bytes of every slice.
type stallWriter struct {
	stalled, resume chan struct{}
	max             int
	writes          int
}

func (w *stallWriter) Write(p []byte) (int, error) {
	if w.writes++; w.writes == 1 {
		close(w.stalled)
		<-w.resume
	}
	return min(len(p), w.max), nil
}

// resumeAtCleanup closes resume when the test ends unless it is closed by
// then, so that a failing test does not leave its stalled call behind.
func resumeAtCleanup(t *testing.T, resume chan struct{}) {
	t.Cleanup(func() {
		select {
		case <-resume:
		default:
			close(resume)
		}
	})
}

// closeWithin fails t unless c.Close returns within d.
func closeWithin(t *testing.T, c *Conn, d time.Duration, while string) {
	t.Helper()
	ch := make(chan error, 1)
	go func() { ch <- c.Close() }()
	select {
	case <-ch:
	case <-time.After(d):
		t.Fatalf("Close did not return within %v while %s", d, while)
	}
}

func TestIOCopyRoundTripsEverySizeWithPartialReads(t *testing.T) {
	const ringSize = 64 << 10
	maxRecord := ring.MaxRecord(ringSize)
	sources := map[string]func(io.Reader) io.Reader{
		"whole":      func(r io.Reader) io.Reader { return struct{ io.Reader }{r} },
		"one byte":   iotest.OneByteReader,
		"data err":   iotest.DataErrReader,
		"1-4097":     func(r io.Reader) io.Reader { return &chunkReader{r: r, max: 4097, rng: rand.New(rand.NewPCG(1, 2))} },
		"zero first": func(r io.Reader) io.Reader { return &zeroFirst{r: r} },
	}
	for name, wrap := range sources {
		for _, size := range []int{0, 1, maxRecord - 1, maxRecord, maxRecord + 1, 3*ringSize + 17} {
			t.Run(fmt.Sprintf("%s/%d", name, size), func(t *testing.T) {
				lc, dc := pair(t, ListenConfig{RingSize: ringSize}, Dialer{RingSize: ringSize})
				want := make([]byte, size)
				rand.NewChaCha8([32]byte{byte(size)}).Read(want)
				sent := make(chan error, 1)
				go func() {
					n, err := io.Copy(lc, wrap(bytes.NewReader(want)))
					if err == nil && n != int64(size) {
						err = fmt.Errorf("io.Copy into the Conn moved %d bytes; want %d", n, size)
					}
					lc.Close()
					sent <- err
				}()
				var got bytes.Buffer
				n, err := io.Copy(&got, dc)
				if err != nil || n != int64(size) || !bytes.Equal(got.Bytes(), want) {
					t.Fatalf("io.Copy out of the peer = %d bytes, %v; want the %d bytes sent, intact", n, err, size)
				}
				if err := <-sent; err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

func TestReadFromFillsRecordsInPlaceSizedByEachRead(t *testing.T) {
	lc, dc := pair(t, ListenConfig{RingSize: 64 << 10}, Dialer{RingSize: 64 << 10})
	// All of these fit the ring unread, so every Read gets at least its
	// length, and over 4 KiB stays free for the Read that reports io.EOF.
	lens := []int{1, 7, 100, 4096, 4097, 16000, lc.MaxRecord()}
	src := &probeReader{c: lc, lens: slices.Clone(lens)}
	if n, err := lc.ReadFrom(src); err != nil || n != 57061 || !slices.Equal(src.returned, lens) {
		t.Fatalf("ReadFrom = %d, %v with reads %v; want 57061, nil with reads %v", n, err, src.returned, lens)
	}
	if src.outside > 0 {
		t.Fatalf("%d of %d slices given to Read lie outside the segment", src.outside, len(lens))
	}
	for i, want := range lens {
		rec, err := dc.Next()
		if err != nil {
			t.Fatal(err)
		}
		if len(rec) != want {
			t.Fatalf("record %d is %d bytes; want the %d bytes that Read returned", i, len(rec), want)
		}
		dc.Release()
	}
}

func TestWriteToHandsEachRecordSliceFromTheRing(t *testing.T) {
	lc, dc := pair(t, ListenConfig{}, Dialer{})
	lens := []int{1, 100, 4096}
	for _, n := range lens {
		if _, err := lc.Write(make([]byte, n)); err != nil {
			t.Fatal(err)
		}
	}
	lc.Close()
	dst := &probeWriter{c: dc}
	if n, err := dc.WriteTo(dst); err != nil || n != 4197 || !slices.Equal(dst.lens, lens) {
		t.Fatalf("WriteTo = %d, %v with writes %v; want 4197, nil with one write per record %v", n, err, dst.lens, lens)
	}
	if dst.outside > 0 {
		t.Fatalf("%d of %d slices handed to Write lie outside the segment", dst.outside, len(lens))
	}
}

func TestWriteToStartsWithTheRestReadHolds(t *testing.T) {
	lc, dc := pair(t, ListenConfig{}, Dialer{})
	for _, s := range []string{"abcdef", "", "gh"} {
		p, err := lc.Reserve(len(s))
		if err != nil {
			t.Fatal(err)
		}
		copy(p, s)
		if err := lc.Commit(); err != nil {
			t.Fatal(err)
		}
	}
	lc.Close()
	buf := make([]byte, 3)
	if n, err := dc.Read(buf); err != nil || string(buf[:n]) != "abc" {
		t.Fatalf("Read = %q, %v; want abc", buf[:n], err)
	}
	var got bytes.Buffer
	if n, err := dc.WriteTo(&got); err != nil || got.String() != "defgh" {
		t.Fatalf("WriteTo = %d, %v (%q); want the held def, then gh, then nil at the peer's close", n, err, got.String())
	}
}

func TestReadFromFillsMostOfTheRingWhenThePeerStalls(t *testing.T) {
	lc, _ := pair(t, ListenConfig{RingSize: 64 << 10}, Dialer{RingSize: 64 << 10})
	lc.SetWriteDeadline(time.Now().Add(300 * time.Millisecond))
	n, err := lc.ReadFrom(capReader(1 << 10))
	if floor := int64(lc.RingSize() * 3 / 4); !isTimeout(err) || n <= floor {
		t.Fatalf("ReadFrom with the peer stalled = %d, %v; want more than %d bytes, then os.ErrDeadlineExceeded", n, err, floor)
	}
}

func TestReadFromDeliversBytesReadBeforeTheSourceError(t *testing.T) {
	want := make([]byte, 1000)
	rand.NewChaCha8([32]byte{5}).Read(want)
	for name, src := range map[string]io.Reader{
		"error after data": io.MultiReader(bytes.NewReader(want), iotest.ErrReader(errSource)),
		"error with data":  &errReader{data: want, err: errSource},
	} {
		t.Run(name, func(t *testing.T) {
			lc, dc := pair(t, ListenConfig{}, Dialer{})
			if n, err := lc.ReadFrom(src); n != 1000 || err != errSource {
				t.Fatalf("ReadFrom = %d, %v; want 1000, the source's error", n, err)
			}
			got := make([]byte, 1000)
			if _, err := io.ReadFull(dc, got); err != nil || !bytes.Equal(got, want) {
				t.Fatalf("peer ReadFull = %v; want the 1000 bytes read before the error", err)
			}
		})
	}
}

func TestLocalCloseWakesBlockedReadFromAndWriteToWithNetErrClosed(t *testing.T) {
	_, dc := pair(t, ListenConfig{RingSize: 64 << 10}, Dialer{RingSize: 64 << 10, Spin: -1})
	ch := make(chan error, 2)
	go func() {
		_, err := dc.ReadFrom(fillReader{}) // fills the ring, then waits for room
		ch <- err
	}()
	go func() {
		_, err := dc.WriteTo(io.Discard)
		ch <- err
	}()
	awaitSendParked(t, dc)
	awaitReceiveParked(t, dc)
	dc.Close()
	for range 2 {
		if err := waitErr(t, ch, time.Second); err != net.ErrClosed {
			t.Fatalf("blocked ReadFrom or WriteTo = %v; want net.ErrClosed", err)
		}
	}
}

func TestCloseReturnsWhileWriteToIsInsideItsDestination(t *testing.T) {
	lc, dc := pair(t, ListenConfig{}, Dialer{})
	if _, err := lc.Write([]byte("record")); err != nil {
		t.Fatal(err)
	}
	dst := &stallWriter{stalled: make(chan struct{}), resume: make(chan struct{}), max: 1 << 30}
	resumeAtCleanup(t, dst.resume)
	ch := make(chan error, 1)
	go func() {
		_, err := dc.WriteTo(dst)
		ch <- err
	}()
	<-dst.stalled
	closeWithin(t, dc, time.Second, "WriteTo was inside its destination's Write")
	close(dst.resume)
	if err := waitErr(t, ch, time.Second); err != net.ErrClosed {
		t.Fatalf("WriteTo once its destination returned = %v; want net.ErrClosed", err)
	}
	if !dc.unmapped.Load() {
		t.Fatal("segment still mapped after Close and WriteTo returned")
	}
}

func TestCloseReturnsWhileReadFromIsInsideItsSource(t *testing.T) {
	_, dc := pair(t, ListenConfig{}, Dialer{})
	src := &stallReader{stalled: make(chan struct{}), resume: make(chan struct{}), rest: strings.NewReader("")}
	resumeAtCleanup(t, src.resume)
	ch := make(chan error, 1)
	go func() {
		_, err := dc.ReadFrom(src)
		ch <- err
	}()
	<-src.stalled
	closeWithin(t, dc, time.Second, "ReadFrom was inside its source's Read")
	close(src.resume)
	if err := waitErr(t, ch, time.Second); err != net.ErrClosed {
		t.Fatalf("ReadFrom once its source returned = %v; want net.ErrClosed", err)
	}
	if !dc.unmapped.Load() {
		t.Fatal("segment still mapped after Close and ReadFrom returned")
	}
}

func TestStreamCallInProgressAtCloseReleasesTheRecordItLeavesPartlyConsumed(t *testing.T) {
	lc, dc := pair(t, ListenConfig{}, Dialer{})
	if _, err := lc.Write([]byte("abcdef")); err != nil {
		t.Fatal(err)
	}
	dst := &stallWriter{stalled: make(chan struct{}), resume: make(chan struct{}), max: 3}
	resumeAtCleanup(t, dst.resume)
	ch := make(chan error, 1)
	go func() {
		_, err := dc.WriteTo(dst)
		ch <- err
	}()
	<-dst.stalled
	closeWithin(t, dc, time.Second, "WriteTo was inside its destination's Write")
	close(dst.resume)
	if err := waitErr(t, ch, time.Second); err != io.ErrShortWrite {
		t.Fatalf("WriteTo = %v; want io.ErrShortWrite, leaving the record partly consumed", err)
	}
	if !dc.unmapped.Load() {
		t.Fatal("segment still mapped after Close and the WriteTo holding a partly consumed record returned")
	}
}

func TestCloseRacingReadAndWriteToOverAPartlyConsumedRecordAlwaysUnmaps(t *testing.T) {
	errStop := errors.New("stop")
	l, path := listen(t, ListenConfig{RingSize: 64 << 10})
	for i := range 2000 {
		served, dialed := serve(t, l, path, 1)
		lc, dc := served[0], dialed[0]
		if _, err := lc.Write(make([]byte, 4096)); err != nil {
			t.Fatal(err)
		}
		if _, err := dc.Read(make([]byte, 1)); err != nil {
			t.Fatal(err)
		}
		errs := make(chan error, 2)
		go func() {
			buf := make([]byte, 1)
			for {
				if _, err := dc.Read(buf); err != nil {
					errs <- err
					return
				}
			}
		}()
		go func() {
			// Yielding inside Write keeps WriteTo in progress across Close.
			w := writerFunc(func([]byte) (int, error) {
				for range i % 4 {
					runtime.Gosched()
				}
				return 1, errStop
			})
			for {
				if _, err := dc.WriteTo(w); err != errStop {
					errs <- err
					return
				}
			}
		}()
		for range i % 32 {
			runtime.Gosched()
		}
		dc.Close()
		for range 2 {
			if err := waitErr(t, errs, 5*time.Second); err != net.ErrClosed {
				t.Fatalf("round %d: Read or WriteTo racing Close = %v; want net.ErrClosed", i, err)
			}
		}
		if !dc.unmapped.Load() {
			t.Fatalf("round %d: segment still mapped after Close, Read and WriteTo returned", i)
		}
		lc.Close()
	}
}

// stallReadFrom starts c.ReadFrom over a stallReader and returns once
// ReadFrom is inside the stalled Read, which returns when the test ends.
func stallReadFrom(t *testing.T, c *Conn) {
	t.Helper()
	src := &stallReader{stalled: make(chan struct{}), resume: make(chan struct{}), rest: strings.NewReader("")}
	resumeAtCleanup(t, src.resume)
	go c.ReadFrom(src)
	<-src.stalled
}

// stallWriteTo has peer send rec, starts c.WriteTo into a stallWriter taking
// at most max bytes per Write, and returns the writer's resume once WriteTo
// is inside its first Write; resume is closed when the test ends at the
// latest.
func stallWriteTo(t *testing.T, peer, c *Conn, rec string, max int) chan struct{} {
	t.Helper()
	if _, err := peer.Write([]byte(rec)); err != nil {
		t.Fatal(err)
	}
	dst := &stallWriter{stalled: make(chan struct{}), resume: make(chan struct{}), max: max}
	resumeAtCleanup(t, dst.resume)
	go c.WriteTo(dst)
	<-dst.stalled
	return dst.resume
}

// goWrite runs c.Write(p) in the background and returns its error's channel.
func goWrite(c *Conn, p []byte) <-chan error {
	ch := make(chan error, 1)
	go func() {
		_, err := c.Write(p)
		ch <- err
	}()
	return ch
}

// readResult is what a Read returned.
type readResult struct {
	data string
	err  error
}

// goRead runs c.Read into an 8-byte buffer in the background and returns its
// result's channel.
func goRead(c *Conn) <-chan readResult {
	ch := make(chan readResult, 1)
	go func() {
		buf := make([]byte, 8)
		n, err := c.Read(buf)
		ch <- readResult{string(buf[:n]), err}
	}()
	return ch
}

// waitRead fails t unless a Read result arrives on ch within d.
func waitRead(t *testing.T, ch <-chan readResult, d time.Duration) readResult {
	t.Helper()
	select {
	case r := <-ch:
		return r
	case <-time.After(d):
		t.Fatalf("queued Read did not return within %v", d)
		return readResult{}
	}
}

func TestWriteQueuedBehindReadFromInItsSourceFailsAtItsDeadline(t *testing.T) {
	_, dc := pair(t, ListenConfig{}, Dialer{})
	stallReadFrom(t, dc)
	ch := goWrite(dc, []byte("x"))
	awaitQueued(t, &dc.wgate)
	dc.SetWriteDeadline(time.Now().Add(100 * time.Millisecond))
	if err := waitErr(t, ch, time.Second); err != os.ErrDeadlineExceeded {
		t.Fatalf("Write queued behind ReadFrom = %v; want os.ErrDeadlineExceeded", err)
	}
}

func TestWriteQueuedBehindReadFromInItsSourceFailsWithNetErrClosedOnClose(t *testing.T) {
	_, dc := pair(t, ListenConfig{}, Dialer{})
	stallReadFrom(t, dc)
	ch := goWrite(dc, []byte("x"))
	awaitQueued(t, &dc.wgate)
	closeWithin(t, dc, time.Second, "ReadFrom was inside its source's Read")
	if err := waitErr(t, ch, time.Second); err != net.ErrClosed {
		t.Fatalf("Write queued behind ReadFrom = %v; want net.ErrClosed", err)
	}
}

func TestWriteQueuedBehindReadFromInItsSourceFailsAtOnceWithErrPeerClosed(t *testing.T) {
	lc, dc := pair(t, ListenConfig{}, Dialer{})
	stallReadFrom(t, dc)
	ch := goWrite(dc, []byte("x"))
	awaitQueued(t, &dc.wgate)
	lc.Close()
	if err := waitErr(t, ch, time.Second); !errors.Is(err, ErrPeerClosed) {
		t.Fatalf("Write queued behind ReadFrom = %v; want ErrPeerClosed", err)
	}
}

func TestReadQueuedBehindWriteToInItsDestinationFailsAtItsDeadline(t *testing.T) {
	lc, dc := pair(t, ListenConfig{}, Dialer{})
	stallWriteTo(t, lc, dc, "record", 1<<30)
	ch := goRead(dc)
	awaitQueued(t, &dc.rgate)
	dc.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
	if r := waitRead(t, ch, time.Second); r.err != os.ErrDeadlineExceeded {
		t.Fatalf("Read queued behind WriteTo = %q, %v; want os.ErrDeadlineExceeded", r.data, r.err)
	}
}

func TestReadQueuedBehindWriteToInItsDestinationFailsWithNetErrClosedOnClose(t *testing.T) {
	lc, dc := pair(t, ListenConfig{}, Dialer{})
	stallWriteTo(t, lc, dc, "record", 1<<30)
	ch := goRead(dc)
	awaitQueued(t, &dc.rgate)
	closeWithin(t, dc, time.Second, "WriteTo was inside its destination's Write")
	if r := waitRead(t, ch, time.Second); r.err != net.ErrClosed {
		t.Fatalf("Read queued behind WriteTo = %q, %v; want net.ErrClosed", r.data, r.err)
	}
}

func TestReadQueuedBehindWriteToKeepsItsPlaceAfterThePeerCloses(t *testing.T) {
	lc, dc := pair(t, ListenConfig{}, Dialer{})
	resume := stallWriteTo(t, lc, dc, "abcdef", 3)
	ch := goRead(dc)
	awaitQueued(t, &dc.rgate)
	lc.Close()
	<-dc.Done()
	select {
	case r := <-ch:
		t.Fatalf("Read queued behind WriteTo = %q, %v at the peer's close; want it to wait, as the stream can still be drained", r.data, r.err)
	case <-time.After(50 * time.Millisecond):
	}
	close(resume)
	if r := waitRead(t, ch, time.Second); r.err != nil || r.data != "def" {
		t.Fatalf("Read queued behind WriteTo = %q, %v; want def, the rest WriteTo left", r.data, r.err)
	}
}

func TestGateAdmitsOneCallAtATimeWhileWaitersGiveUp(t *testing.T) {
	g := gate{baton: make(chan struct{}, 1)}
	var inside, overlaps, taken, gaveUp atomic.Int64
	// Holds and deadlines of a few microseconds make a waiter's deadline
	// often pass just as the holder leaving wakes it.
	const workers = 3
	done := make(chan error, workers)
	for w := range workers {
		go func() {
			rng := rand.New(rand.NewPCG(uint64(w), 1))
			for range 5000 {
				var dl deadline
				dl.set(time.Now().Add(time.Duration(rng.IntN(10)) * time.Microsecond))
				if !g.lock(nil, &dl) {
					gaveUp.Add(1)
					continue
				}
				if inside.Add(1) != 1 {
					overlaps.Add(1)
				}
				for until := time.Now().Add(time.Duration(rng.IntN(10)) * time.Microsecond); time.Now().Before(until); {
				}
				inside.Add(-1)
				taken.Add(1)
				g.unlock()
			}
			done <- nil
		}()
	}
	for range workers {
		waitErr(t, done, 10*time.Second)
	}
	if overlaps.Load() > 0 {
		t.Fatalf("%d times two calls held the gate at once", overlaps.Load())
	}
	if !gateIdle(&g) {
		t.Fatalf("gate left with state %#x and %d wake-ups; want it free", g.state.Load(), len(g.baton))
	}
	t.Logf("%d calls took the gate, %d gave up", taken.Load(), gaveUp.Load())
}

// gateIdle reports whether no call holds or waits for g, with a wake-up left
// only for a woken call that has yet to contend, as when every waiting call
// gave up.
func gateIdle(g *gate) bool {
	s := g.state.Load()
	return s&^gateWoken == 0 && int64(len(g.baton)) == s&gateWoken>>1
}

func TestGateServesEveryWaitingCallOneAtATimeUnderStress(t *testing.T) {
	g := gate{baton: make(chan struct{}, 1)}
	var inside, taken, gaveUp atomic.Int64
	const workers = 8
	seed := uint64(time.Now().UnixNano())
	// Every round ends with no call left: a waiting call nobody woke hangs it.
	rounds := 0
	for end := time.Now().Add(2 * time.Second); time.Now().Before(end); rounds++ {
		errs := make(chan error, workers)
		for w := range workers {
			rng := rand.New(rand.NewPCG(seed, uint64(rounds*workers+w)))
			go func() {
				for range 1 + rng.IntN(64) {
					var dl deadline
					if rng.IntN(2) == 0 {
						dl.set(time.Now().Add(time.Duration(rng.IntN(50)) * time.Microsecond))
					}
					if !g.lock(nil, &dl) {
						if !dl.expired.Load() {
							errs <- errors.New("a call gave up before its deadline passed")
							return
						}
						gaveUp.Add(1)
						continue
					}
					if inside.Add(1) != 1 {
						errs <- errors.New("two calls held the gate at once")
						return
					}
					for until := time.Now().Add(time.Duration(rng.IntN(20)) * time.Microsecond); time.Now().Before(until); {
					}
					inside.Add(-1)
					taken.Add(1)
					g.unlock()
				}
				errs <- nil
			}()
		}
		for range workers {
			if err := waitErr(t, errs, time.Minute); err != nil {
				t.Fatalf("round %d (seed %d): %v", rounds, seed, err)
			}
		}
		if !gateIdle(&g) {
			t.Fatalf("round %d (seed %d): gate left with state %#x and %d wake-ups after every call returned", rounds, seed, g.state.Load(), len(g.baton))
		}
	}
	t.Logf("%d rounds: %d calls took the gate, %d gave up", rounds, taken.Load(), gaveUp.Load())
}

// BenchmarkContendedWrite64B has 1, 2 or 4 goroutines Write 64 B in a tight
// loop on one connection, b.N Writes in all, while one reader drains it.
func BenchmarkContendedWrite64B(b *testing.B) {
	for _, writers := range []int{1, 2, 4} {
		b.Run(fmt.Sprintf("writers=%d", writers), func(b *testing.B) {
			lc, dc := pair(b, ListenConfig{}, Dialer{})
			drained := make(chan error, 1)
			go func() {
				_, err := io.Copy(io.Discard, lc)
				drained <- err
			}()
			msg := make([]byte, 64)
			per := (b.N + writers - 1) / writers
			errs := make(chan error, writers)
			b.ResetTimer()
			for range writers {
				go func() {
					for range per {
						if _, err := dc.Write(msg); err != nil {
							errs <- err
							return
						}
					}
					errs <- nil
				}()
			}
			for range writers {
				if err := <-errs; err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			dc.Close()
			if err := <-drained; err != nil {
				b.Fatal(err)
			}
		})
	}
}

func TestPeerCrashFailsReadFromAndEndsWriteToWithErrPeerClosed(t *testing.T) {
	lc, dc := pair(t, ListenConfig{RingSize: 64 << 10}, Dialer{RingSize: 64 << 10, Spin: -1})
	want := []byte("committed before the crash")
	if _, err := lc.Write(want); err != nil {
		t.Fatal(err)
	}
	ch := make(chan error, 1)
	go func() {
		_, err := dc.ReadFrom(fillReader{})
		ch <- err
	}()
	awaitSendParked(t, dc)
	lc.link.Close() // vanish without Bye
	if err := waitErr(t, ch, time.Second); !errors.Is(err, ErrPeerClosed) {
		t.Fatalf("blocked ReadFrom = %v; want ErrPeerClosed", err)
	}
	var got bytes.Buffer
	if n, err := dc.WriteTo(&got); !errors.Is(err, ErrPeerClosed) || !bytes.Equal(got.Bytes(), want) {
		t.Fatalf("WriteTo after the crash = %d, %v (%q); want the bytes the peer committed, then ErrPeerClosed", n, err, got.String())
	}
}

// readFromPastPeerClose runs lc.ReadFrom over a stallReader whose Reads
// after the stall come from rest: while the source stalls, the peer takes
// the one record ReadFrom committed and closes, so the reservation ReadFrom
// makes next meets the peer's close. It returns ReadFrom's result and the
// bytes the peer took.
func readFromPastPeerClose(t *testing.T, rest io.Reader) (n, sent int64, err error) {
	t.Helper()
	lc, dc := pair(t, ListenConfig{RingSize: 64 << 10}, Dialer{RingSize: 64 << 10})
	src := &stallReader{stalled: make(chan struct{}), resume: make(chan struct{}), rest: rest}
	ch := make(chan error, 1)
	go func() {
		var err error
		n, err = lc.ReadFrom(src)
		ch <- err
	}()
	<-src.stalled
	rec, err := dc.Next()
	if err != nil {
		t.Fatal(err)
	}
	sent = int64(len(rec))
	dc.Release()
	dc.Close()
	<-lc.Done()
	close(src.resume)
	err = waitErr(t, ch, time.Second)
	return n, sent, err
}

func TestReadFromOfAnExhaustedSourceEndsWithNilOnceThePeerTookAllAndClosed(t *testing.T) {
	n, sent, err := readFromPastPeerClose(t, strings.NewReader(""))
	if err != nil || n != sent {
		t.Fatalf("ReadFrom = %d, %v; want %d, nil: the peer took every byte sent and the source has no more", n, err, sent)
	}
}

func TestReadFromWithSourceDataLeftFailsWithErrPeerClosedOnceThePeerClosed(t *testing.T) {
	n, sent, err := readFromPastPeerClose(t, strings.NewReader("left behind"))
	if !errors.Is(err, ErrPeerClosed) || n != sent {
		t.Fatalf("ReadFrom = %d, %v; want %d, ErrPeerClosed: the source has data the peer will never take", n, err, sent)
	}
}

func TestWriteToKeepsTheUnwrittenRestForRead(t *testing.T) {
	errSink := errors.New("sink failed")
	for name, tc := range map[string]struct{ werr, want error }{
		"writer error": {errSink, errSink},
		"short write":  {nil, io.ErrShortWrite},
	} {
		t.Run(name, func(t *testing.T) {
			lc, dc := pair(t, ListenConfig{}, Dialer{})
			if _, err := lc.Write([]byte("abcdef")); err != nil {
				t.Fatal(err)
			}
			w := &limitWriter{n: 3, err: tc.werr}
			if n, err := dc.WriteTo(w); n != 3 || err != tc.want || string(w.got) != "abc" {
				t.Fatalf("WriteTo = %d, %v (wrote %q); want 3, %v (wrote abc)", n, err, w.got, tc.want)
			}
			buf := make([]byte, 8)
			if n, err := dc.Read(buf); err != nil || string(buf[:n]) != "def" {
				t.Fatalf("Read after the failed WriteTo = %q, %v; want the unwritten def", buf[:n], err)
			}
		})
	}
}

func TestWriteSucceedsAfterReadFromReturns(t *testing.T) {
	for name, run := range map[string]func(*Conn) (int64, error){
		"source EOF":   func(c *Conn) (int64, error) { return c.ReadFrom(strings.NewReader("")) },
		"source error": func(c *Conn) (int64, error) { return c.ReadFrom(iotest.ErrReader(errSource)) },
		"write deadline": func(c *Conn) (int64, error) {
			c.SetWriteDeadline(time.Now().Add(-time.Second))
			defer c.SetWriteDeadline(time.Time{})
			return c.ReadFrom(strings.NewReader("x"))
		},
	} {
		t.Run(name, func(t *testing.T) {
			lc, dc := pair(t, ListenConfig{}, Dialer{})
			if n, _ := run(lc); n != 0 {
				t.Fatalf("ReadFrom sent %d bytes; want 0", n)
			}
			if _, err := lc.Write([]byte("after")); err != nil {
				t.Fatalf("Write after ReadFrom returned: %v", err)
			}
			buf := make([]byte, 8)
			if n, err := dc.Read(buf); err != nil || string(buf[:n]) != "after" {
				t.Fatalf("Read = %q, %v; want after", buf[:n], err)
			}
		})
	}
}

func TestIOCopyUsesReadFromAndWriteTo(t *testing.T) {
	lc, dc := pair(t, ListenConfig{}, Dialer{})
	src := &probeReader{c: lc, lens: []int{100, 100, 100}}
	if n, err := io.Copy(lc, src); err != nil || n != 300 || src.outside > 0 {
		t.Fatalf("io.Copy(conn, src) = %d, %v with %d reads into private memory; want 300 bytes read straight into the segment", n, err, src.outside)
	}
	lc.Close()
	dst := &probeWriter{c: dc}
	if n, err := io.Copy(dst, dc); err != nil || n != 300 || dst.outside > 0 {
		t.Fatalf("io.Copy(dst, conn) = %d, %v with %d writes from private memory; want 300 bytes written straight from the segment", n, err, dst.outside)
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
