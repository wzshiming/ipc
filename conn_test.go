package ipc

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/wzshiming/ipc/internal/control"
	"github.com/wzshiming/ipc/internal/ring"
	"github.com/wzshiming/ipc/internal/shm"
)

// sockPath returns a short socket path: macOS limits sun_path to 104 bytes.
func sockPath(t testing.TB) string {
	dir, err := os.MkdirTemp("", "ipc")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	return filepath.Join(dir, "s")
}

func listen(t testing.TB, lcfg ListenConfig) (*Listener, string) {
	path := sockPath(t)
	l, err := lcfg.Listen(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	return l, path
}

// pair returns the listener-side and dialer-side ends of one connection.
func pair(t testing.TB, lcfg ListenConfig, d Dialer) (lc, dc *Conn) {
	l, path := listen(t, lcfg)
	l.SetDeadline(time.Now().Add(10 * time.Second))
	d.Timeout = 10 * time.Second
	type result struct {
		c   net.Conn
		err error
	}
	ch := make(chan result, 1)
	go func() {
		c, err := l.Accept()
		ch <- result{c, err}
	}()
	dc, err := d.Dial(path)
	if err != nil {
		t.Fatal("Dial:", err)
	}
	r := <-ch
	if r.err != nil {
		dc.Close()
		t.Fatal("Accept:", r.err)
	}
	lc = r.c.(*Conn)
	t.Cleanup(func() {
		lc.Close()
		dc.Close()
	})
	return lc, dc
}

// acceptOne accepts one connection on l in the background; the test's
// cleanup closes l and the connection.
func acceptOne(t *testing.T, l *Listener) {
	ch := make(chan net.Conn, 1)
	go func() {
		c, _ := l.Accept()
		ch <- c
	}()
	t.Cleanup(func() {
		l.Close()
		if c := <-ch; c != nil {
			c.Close()
		}
	})
}

func TestDialerRequestIsGrantedWithoutListenerCap(t *testing.T) {
	lc, dc := pair(t, ListenConfig{}, Dialer{RingSize: 8 << 20})
	for _, c := range []*Conn{lc, dc} {
		if c.RingSize() != 8<<20 {
			t.Fatalf("RingSize = %d; want the requested %d", c.RingSize(), 8<<20)
		}
		if c.MaxRecord() != ring.MaxRecord(8<<20) {
			t.Fatalf("MaxRecord = %d; want %d", c.MaxRecord(), ring.MaxRecord(8<<20))
		}
	}
}

func TestDialerRequestIsClampedToListenerCap(t *testing.T) {
	lc, dc := pair(t, ListenConfig{RingSize: 128 << 10}, Dialer{RingSize: 1 << 20})
	for _, c := range []*Conn{lc, dc} {
		if c.RingSize() != 128<<10 {
			t.Fatalf("RingSize = %d; want the cap %d", c.RingSize(), 128<<10)
		}
		if c.MaxRecord() != ring.MaxRecord(128<<10) {
			t.Fatalf("MaxRecord = %d; want %d", c.MaxRecord(), ring.MaxRecord(128<<10))
		}
	}
}

func TestZeroDialerRequestsFourMiB(t *testing.T) {
	lc, dc := pair(t, ListenConfig{}, Dialer{})
	for _, c := range []*Conn{lc, dc} {
		if c.RingSize() != 4<<20 {
			t.Fatalf("RingSize = %d; want %d", c.RingSize(), 4<<20)
		}
	}
}

func TestPackageListenAndDialUseDefaults(t *testing.T) {
	path := sockPath(t)
	l, err := Listen(path)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	acceptOne(t, l)
	c, err := Dial(path)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if c.RingSize() != 4<<20 {
		t.Fatalf("RingSize = %d; want %d", c.RingSize(), 4<<20)
	}
}

func TestNilListenConfigAndDialerUseDefaults(t *testing.T) {
	path := sockPath(t)
	var lc *ListenConfig
	l, err := lc.Listen(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	acceptOne(t, l)
	var d *Dialer
	c, err := d.DialContext(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if c.RingSize() != 4<<20 {
		t.Fatalf("RingSize = %d; want %d", c.RingSize(), 4<<20)
	}
}

func TestListenConfigListenWithDoneCtxFails(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if l, err := (&ListenConfig{}).Listen(ctx, sockPath(t)); !errors.Is(err, context.Canceled) {
		if l != nil {
			l.Close()
		}
		t.Fatalf("Listen = %v; want context.Canceled", err)
	}
}

// silentEndpoint listens on a socket but never takes a link from it, so a
// dialer's Hello gets no answer.
func silentEndpoint(t *testing.T) string {
	t.Helper()
	path := sockPath(t)
	cl, err := control.Listen(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cl.Close() })
	return path
}

func TestDialerTimeoutAbortsDialToAnEndpointThatNeverAnswers(t *testing.T) {
	path := silentEndpoint(t)
	start := time.Now()
	c, err := (&Dialer{Timeout: 50 * time.Millisecond}).Dial(path)
	if err == nil {
		c.Close()
	}
	if err != os.ErrDeadlineExceeded {
		t.Fatalf("Dial = %v; want os.ErrDeadlineExceeded", err)
	}
	if d := time.Since(start); d > time.Second {
		t.Fatalf("Dial returned after %v; want about 50ms", d)
	}
}

func TestDialContextWithExpiringCtxAbortsDialToAnEndpointThatNeverAnswers(t *testing.T) {
	path := silentEndpoint(t)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	c, err := (&Dialer{Timeout: time.Minute}).DialContext(ctx, path)
	if err == nil {
		c.Close()
	}
	if err != context.DeadlineExceeded {
		t.Fatalf("DialContext = %v; want context.DeadlineExceeded", err)
	}
	if d := time.Since(start); d > time.Second {
		t.Fatalf("DialContext returned after %v; want about 50ms", d)
	}
}

func TestListenRejectsOutOfRangeCap(t *testing.T) {
	for _, lcfg := range []ListenConfig{
		{RingSize: -1},
		{RingSize: 100000},
		{RingSize: 32 << 10},
		{RingSize: 1 << 30}, // two such rings exceed the segment limit
	} {
		l, err := lcfg.Listen(context.Background(), sockPath(t))
		if err == nil {
			l.Close()
			t.Errorf("Listen(%+v) succeeded", lcfg)
		}
	}
}

func TestAddrsAreIPCPaths(t *testing.T) {
	l, path := listen(t, ListenConfig{})
	if a := l.Addr(); a.Network() != "ipc" || a.String() != path {
		t.Fatalf("Listener.Addr = %s:%s", a.Network(), a.String())
	}
	acceptOne(t, l)
	dc, err := Dial(path)
	if err != nil {
		t.Fatal(err)
	}
	defer dc.Close()
	for _, a := range []net.Addr{dc.LocalAddr(), dc.RemoteAddr()} {
		if a.Network() != "ipc" || a.String() != path {
			t.Fatalf("Conn addr = %s:%s", a.Network(), a.String())
		}
	}
}

// waitErr fails the test unless an error arrives on ch within d.
func waitErr(t *testing.T, ch <-chan error, d time.Duration) error {
	t.Helper()
	select {
	case err := <-ch:
		return err
	case <-time.After(d):
		t.Fatalf("blocked operation did not return within %v", d)
		return nil
	}
}

func TestInterruptedParkedNextKeepsData(t *testing.T) {
	lc, dc := pair(t, ListenConfig{}, Dialer{Spin: -1})
	ch := make(chan error, 1)
	go func() {
		_, err := dc.Next()
		ch <- err
	}()
	time.Sleep(50 * time.Millisecond)
	dc.SetReadDeadline(time.Now())
	if err := waitErr(t, ch, 100*time.Millisecond); err != os.ErrDeadlineExceeded {
		t.Fatalf("Next after deadline = %v; want os.ErrDeadlineExceeded", err)
	}
	dc.SetReadDeadline(time.Time{})
	if _, err := lc.Write([]byte("after")); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 16)
	n, err := dc.Read(buf)
	if err != nil || string(buf[:n]) != "after" {
		t.Fatalf("Read = %q, %v; want after", buf[:n], err)
	}
}

func TestInterruptedParkedReserveKeepsData(t *testing.T) {
	lc, dc := pair(t, ListenConfig{RingSize: 64 << 10}, Dialer{RingSize: 64 << 10, Spin: -1})
	tx, rx := dc, lc
	var sent uint64
	for {
		p, err := tx.TryReserve(1000)
		if errors.Is(err, ErrWouldBlock) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		putSeq(p, sent)
		sent++
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
	}
	ch := make(chan error, 1)
	go func() {
		_, err := tx.Reserve(1000)
		ch <- err
	}()
	time.Sleep(50 * time.Millisecond)
	tx.SetWriteDeadline(time.Now())
	if err := waitErr(t, ch, 100*time.Millisecond); err != os.ErrDeadlineExceeded {
		t.Fatalf("Reserve after deadline = %v; want os.ErrDeadlineExceeded", err)
	}
	tx.SetWriteDeadline(time.Time{})
	p := make([]byte, 1000)
	putSeq(p, sent)
	go func() {
		_, err := tx.Write(p)
		ch <- err
	}()
	for want := uint64(0); want <= sent; want++ {
		rec, err := rx.Next()
		if err != nil {
			t.Fatal(err)
		}
		if got := getSeq(rec); got != want {
			t.Fatalf("record seq %d; want %d", got, want)
		}
		rx.Release()
	}
	if err := waitErr(t, ch, time.Second); err != nil {
		t.Fatal("Write after deadline:", err)
	}
}

func putSeq(p []byte, seq uint64) {
	for i := range 8 {
		p[i] = byte(seq >> (8 * i))
	}
}

func getSeq(p []byte) (seq uint64) {
	for i := range 8 {
		seq |= uint64(p[i]) << (8 * i)
	}
	return seq
}

func TestPeerCloseWakesBlockedNextWithEOF(t *testing.T) {
	lc, dc := pair(t, ListenConfig{}, Dialer{Spin: -1})
	ch := make(chan error, 1)
	go func() {
		_, err := dc.Next()
		ch <- err
	}()
	time.Sleep(20 * time.Millisecond)
	lc.Close()
	if err := waitErr(t, ch, time.Second); err != io.EOF {
		t.Fatalf("Next = %v; want io.EOF", err)
	}
	<-dc.Done()
	if !errors.Is(dc.Err(), ErrPeerClosed) || !errors.Is(lc.Err(), net.ErrClosed) {
		t.Fatalf("Err = %v (dialer), %v (listener); want ErrPeerClosed, net.ErrClosed", dc.Err(), lc.Err())
	}
}

func TestPeerCloseWakesBlockedReserveWithErrPeerClosed(t *testing.T) {
	lc, dc := pair(t, ListenConfig{RingSize: 64 << 10}, Dialer{RingSize: 64 << 10, Spin: -1})
	for {
		if _, err := dc.TryReserve(4000); err != nil {
			break
		}
		dc.Commit()
	}
	ch := make(chan error, 1)
	go func() {
		_, err := dc.Reserve(4000)
		ch <- err
	}()
	time.Sleep(20 * time.Millisecond)
	lc.Close()
	if err := waitErr(t, ch, time.Second); !errors.Is(err, ErrPeerClosed) {
		t.Fatalf("Reserve = %v; want ErrPeerClosed", err)
	}
}

// commitSeq commits n records of size bytes numbered 0..n-1 on c.
func commitSeq(t *testing.T, c *Conn, n, size int) {
	t.Helper()
	for i := range n {
		p, err := c.TryReserve(size)
		if err != nil {
			t.Fatalf("TryReserve record %d: %v", i, err)
		}
		putSeq(p, uint64(i))
		if err := c.Commit(); err != nil {
			t.Fatal(err)
		}
	}
}

// drainSeq reads n records numbered 0..n-1 from c, alternating TryNext and
// Next, then expects end from Next, TryNext and Read.
func drainSeq(t *testing.T, c *Conn, n int, end error) {
	t.Helper()
	c.SetReadDeadline(time.Now().Add(5 * time.Second))
	for i := range n {
		var rec []byte
		var err error
		if i%2 == 0 {
			rec, err = c.TryNext()
		} else {
			rec, err = c.Next()
		}
		if err != nil {
			t.Fatalf("record %d after peer close: %v", i, err)
		}
		if got := getSeq(rec); got != uint64(i) {
			t.Fatalf("record seq %d; want %d", got, i)
		}
		if err := c.Release(); err != nil {
			t.Fatalf("Release record %d after peer close: %v", i, err)
		}
	}
	if _, err := c.Next(); err != end {
		t.Fatalf("Next on drained ring = %v; want %v", err, end)
	}
	if _, err := c.TryNext(); err != end {
		t.Fatalf("TryNext on drained ring = %v; want %v", err, end)
	}
	if _, err := c.Read(make([]byte, 8)); err != end {
		t.Fatalf("Read on drained ring = %v; want %v", err, end)
	}
}

func TestPeerCloseLeavesCommittedRecordsDrainableThenEOF(t *testing.T) {
	lc, dc := pair(t, ListenConfig{}, Dialer{})
	commitSeq(t, lc, 1000, 1<<10)
	lc.Close()
	<-dc.Done()
	drainSeq(t, dc, 1000, io.EOF)
	if !errors.Is(dc.Err(), ErrPeerClosed) {
		t.Fatalf("Err = %v; want ErrPeerClosed", dc.Err())
	}
}

func TestParkedNextWokenByPeerCloseReturnsRecordCommittedBeforeIt(t *testing.T) {
	lc, dc := pair(t, ListenConfig{}, Dialer{Spin: -1})
	type result struct {
		seq uint64
		err error
	}
	ch := make(chan result, 1)
	go func() {
		rec, err := dc.Next()
		if err != nil {
			ch <- result{err: err}
			return
		}
		ch <- result{seq: getSeq(rec)}
		dc.Release()
	}()
	time.Sleep(20 * time.Millisecond)
	// Commit without ringing, so only the peer's close wakes the parked Next.
	p, err := lc.prod.TryReserve(8)
	if err != nil {
		t.Fatal(err)
	}
	putSeq(p, 7)
	lc.prod.Commit()
	lc.Close()
	select {
	case r := <-ch:
		if r.err != nil || r.seq != 7 {
			t.Fatalf("Next = seq %d, %v; want the record committed before close", r.seq, r.err)
		}
	case <-time.After(time.Second):
		t.Fatal("parked Next was not woken by the peer's close")
	}
}

func TestPeerCrashLeavesCommittedRecordsDrainableThenErrPeerClosed(t *testing.T) {
	lc, dc := pair(t, ListenConfig{}, Dialer{})
	commitSeq(t, lc, 1000, 1<<10)
	lc.link.Close() // vanish without Bye
	<-dc.Done()
	drainSeq(t, dc, 1000, ErrPeerClosed)
	if !errors.Is(dc.Err(), ErrPeerClosed) {
		t.Fatalf("Err = %v; want ErrPeerClosed", dc.Err())
	}
}

func TestReleaseAfterPeerCloseSucceedsWhenPeerWasParked(t *testing.T) {
	lc, dc := pair(t, ListenConfig{RingSize: 64 << 10, Spin: -1}, Dialer{RingSize: 64 << 10})
	n := 0
	for ; ; n++ {
		p, err := lc.TryReserve(4000)
		if err != nil {
			break
		}
		putSeq(p, uint64(n))
		lc.Commit()
	}
	ch := make(chan error, 1)
	go func() {
		_, err := lc.Reserve(4000)
		ch <- err
	}()
	time.Sleep(20 * time.Millisecond)
	lc.Close()
	waitErr(t, ch, time.Second)
	<-dc.Done()
	drainSeq(t, dc, n, io.EOF)
}

func TestPeerCloseFailsReserveTryReserveAndWriteAtOnce(t *testing.T) {
	lc, dc := pair(t, ListenConfig{}, Dialer{})
	commitSeq(t, lc, 10, 100)
	lc.Close()
	<-dc.Done()
	if _, err := dc.TryReserve(8); !errors.Is(err, ErrPeerClosed) {
		t.Fatalf("TryReserve = %v; want ErrPeerClosed", err)
	}
	if _, err := dc.Reserve(8); !errors.Is(err, ErrPeerClosed) {
		t.Fatalf("Reserve = %v; want ErrPeerClosed", err)
	}
	if _, err := dc.Write([]byte("x")); !errors.Is(err, ErrPeerClosed) {
		t.Fatalf("Write = %v; want ErrPeerClosed", err)
	}
}

func TestLocalCloseAfterPeerCloseUnmapsAndStopsDraining(t *testing.T) {
	lc, dc := pair(t, ListenConfig{}, Dialer{})
	commitSeq(t, lc, 10, 100)
	lc.Close()
	<-dc.readerDone // the peer-close shutdown has finished
	if dc.unmapped.Load() {
		t.Fatal("segment unmapped by peer close; draining needs it until local Close")
	}
	dc.Close()
	if !dc.unmapped.Load() {
		t.Fatal("segment still mapped after local Close")
	}
	if _, err := dc.TryNext(); err != net.ErrClosed {
		t.Fatalf("TryNext after local Close = %v; want net.ErrClosed", err)
	}
	if _, err := dc.TryReserve(8); err != net.ErrClosed {
		t.Fatalf("TryReserve after local Close = %v; want net.ErrClosed", err)
	}
	if err := dc.Err(); err != net.ErrClosed {
		t.Fatalf("Err after local Close = %v; want net.ErrClosed", err)
	}
}

func TestLocalCloseWakesBlockedNextWithNetErrClosed(t *testing.T) {
	_, dc := pair(t, ListenConfig{}, Dialer{Spin: -1})
	ch := make(chan error, 1)
	go func() {
		_, err := dc.Next()
		ch <- err
	}()
	time.Sleep(20 * time.Millisecond)
	dc.Close()
	if err := waitErr(t, ch, time.Second); err != net.ErrClosed {
		t.Fatalf("Next = %v; want net.ErrClosed", err)
	}
	if _, err := dc.TryNext(); err != net.ErrClosed {
		t.Fatalf("TryNext after Close = %v; want net.ErrClosed", err)
	}
	if err := dc.Err(); err != net.ErrClosed {
		t.Fatalf("Err after Close = %v; want net.ErrClosed", err)
	}
}

func TestLocalCloseWakesBlockedReserveWithNetErrClosed(t *testing.T) {
	_, dc := pair(t, ListenConfig{RingSize: 64 << 10}, Dialer{RingSize: 64 << 10, Spin: -1})
	for {
		if _, err := dc.TryReserve(4000); err != nil {
			break
		}
		dc.Commit()
	}
	ch := make(chan error, 1)
	go func() {
		_, err := dc.Reserve(4000)
		ch <- err
	}()
	time.Sleep(20 * time.Millisecond)
	dc.Close()
	if err := waitErr(t, ch, time.Second); err != net.ErrClosed {
		t.Fatalf("Reserve = %v; want net.ErrClosed", err)
	}
}

func TestLocalCloseWakesBlockedReadAndWriteWithNetErrClosed(t *testing.T) {
	_, dc := pair(t, ListenConfig{RingSize: 64 << 10}, Dialer{RingSize: 64 << 10, Spin: -1})
	fill(t, dc)
	ch := make(chan error, 2)
	go func() {
		_, err := dc.Write(make([]byte, 4000))
		ch <- err
	}()
	go func() {
		_, err := dc.Read(make([]byte, 8))
		ch <- err
	}()
	time.Sleep(20 * time.Millisecond)
	dc.Close()
	for range 2 {
		if err := waitErr(t, ch, time.Second); err != net.ErrClosed {
			t.Fatalf("blocked Read or Write = %v; want net.ErrClosed", err)
		}
	}
}

func TestCommitAfterLocalCloseIsNetErrClosed(t *testing.T) {
	_, dc := pair(t, ListenConfig{}, Dialer{})
	if _, err := dc.TryReserve(8); err != nil {
		t.Fatal(err)
	}
	dc.Close()
	if err := dc.Commit(); err != net.ErrClosed {
		t.Fatalf("Commit after Close = %v; want net.ErrClosed", err)
	}
}

func TestCloseDefersUnmapUntilInflightNextReleased(t *testing.T) {
	lc, dc := pair(t, ListenConfig{}, Dialer{})
	if _, err := lc.Write([]byte("hello")); err != nil {
		t.Fatal(err)
	}
	rec, err := dc.Next()
	if err != nil {
		t.Fatal(err)
	}
	dc.Close()
	if dc.unmapped.Load() {
		t.Fatal("segment unmapped while a Next slice is outstanding")
	}
	if string(rec) != "hello" {
		t.Fatalf("in-flight slice reads %q after Close", rec)
	}
	dc.Release()
	if !dc.unmapped.Load() {
		t.Fatal("segment still mapped after the last slice was released")
	}
}

func TestCloseDefersUnmapUntilInflightReserveAborted(t *testing.T) {
	_, dc := pair(t, ListenConfig{}, Dialer{})
	p, err := dc.Reserve(100)
	if err != nil {
		t.Fatal(err)
	}
	dc.Close()
	if dc.unmapped.Load() {
		t.Fatal("segment unmapped while a Reserve slice is outstanding")
	}
	clear(p)
	dc.Abort()
	if !dc.unmapped.Load() {
		t.Fatal("segment still mapped after the reservation was aborted")
	}
}

func TestCloseUnmapsAtOnceWithoutInflightSlices(t *testing.T) {
	lc, _ := pair(t, ListenConfig{}, Dialer{})
	lc.Close()
	if !lc.unmapped.Load() {
		t.Fatal("segment still mapped after Close with nothing outstanding")
	}
}

func TestCloseReleasesRecordReadHoldsPartlyConsumed(t *testing.T) {
	lc, dc := pair(t, ListenConfig{}, Dialer{})
	if _, err := lc.Write([]byte("abcdef")); err != nil {
		t.Fatal(err)
	}
	if _, err := dc.Read(make([]byte, 3)); err != nil {
		t.Fatal(err)
	}
	dc.Close()
	if !dc.unmapped.Load() {
		t.Fatal("segment still mapped after Close while Read held a partly consumed record")
	}
}

// rawHello sends payload as a Hello frame and returns the listener's answer.
func rawHello(t *testing.T, path string, payload []byte) (typ uint8, answer []byte) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	link, err := control.Dial(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer link.Close()
	link.SetDeadline(time.Now().Add(5 * time.Second))
	if err := link.Send(frameHello, payload, nil); err != nil {
		t.Fatal(err)
	}
	typ, answer, fds, err := link.Recv()
	if err != nil {
		t.Fatal(err)
	}
	control.CloseFDs(fds)
	if _, _, _, err := link.Recv(); !errors.Is(err, io.EOF) {
		t.Fatalf("Recv after Reject = %v; want io.EOF (listener closes the link)", err)
	}
	return typ, answer
}

func TestListenerRejectsInvalidHello(t *testing.T) {
	l, path := listen(t, ListenConfig{})
	acceptOne(t, l)
	for name, h := range map[string]hello{
		"version":        {version: protoVersion + 1, ringSize: 64 << 10},
		"ring too small": {version: protoVersion, ringSize: 32 << 10},
		"ring not pow2":  {version: protoVersion, ringSize: 100000},
		"ring too big":   {version: protoVersion, ringSize: 1 << 30},
	} {
		t.Run(name, func(t *testing.T) {
			if typ, reason := rawHello(t, path, h.encode()); typ != frameReject {
				t.Fatalf("frame type %d (%q); want Reject %d", typ, reason, frameReject)
			}
		})
	}
}

func TestListenerRefusesVersionOneHelloAsUnsupportedVersion(t *testing.T) {
	l, path := listen(t, ListenConfig{})
	acceptOne(t, l)
	// Version 1's hello was 20 bytes: version u32 followed by two u64 fields.
	v1 := binary.LittleEndian.AppendUint32(nil, 1)
	v1 = binary.LittleEndian.AppendUint64(v1, 1)
	v1 = binary.LittleEndian.AppendUint64(v1, 4<<20)
	typ, reason := rawHello(t, path, v1)
	if typ != frameReject || string(reason) != "unsupported protocol version" {
		t.Fatalf("frame type %d (%q); want Reject %d (%q)", typ, reason, frameReject, "unsupported protocol version")
	}
}

func TestListenerClosesTheDescriptorsAHelloCarries(t *testing.T) {
	if openFDs(t) < 0 {
		t.Skipf("descriptors cannot be counted on %s", runtime.GOOS)
	}
	l, path := listen(t, ListenConfig{})
	acceptOne(t, l)
	before := openFDs(t)
	link, err := control.Dial(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	defer link.Close()
	var files []*os.File
	var fds []int
	for range 8 {
		f, err := os.Open(os.DevNull)
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		files = append(files, f)
		fds = append(fds, int(f.Fd()))
	}
	if err := link.Send(frameHello, hello{version: protoVersion, ringSize: minRingSize}.encode(), fds); err != nil {
		t.Fatal(err)
	}
	link.SetDeadline(time.Now().Add(5 * time.Second))
	if _, _, _, err := link.Recv(); !errors.Is(err, io.EOF) {
		t.Fatalf("Recv after a Hello carrying descriptors = %v; want io.EOF (the listener drops the dialer)", err)
	}
	link.Close()
	for _, f := range files {
		f.Close()
	}
	if n := openFDs(t); n > before {
		t.Fatalf("%d descriptors open once the listener dropped the dialer, %d before; want the %d it received closed", n, before, len(fds))
	}
}

func TestDialReturnsErrRejected(t *testing.T) {
	l, path := listen(t, ListenConfig{})
	acceptOne(t, l)
	c, err := (&Dialer{RingSize: 100000}).Dial(path)
	if !errors.Is(err, ErrRejected) {
		if c != nil {
			c.Close()
		}
		t.Fatalf("Dial = %v; want ErrRejected", err)
	}
}

func TestDialReturnsErrCorruptForTamperedRingHeaders(t *testing.T) {
	path := sockPath(t)
	cl, err := control.Listen(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	const rs = 64 << 10
	seg, err := shm.Create(int(segmentSize(rs)))
	if err != nil {
		t.Fatal(err)
	}
	defer seg.Close()
	mem := seg.Bytes()
	for i := range ring.HeaderSize {
		mem[i] = 0xab
	}
	go func() {
		link, err := cl.Accept()
		if err != nil {
			return
		}
		defer link.Close()
		if _, _, fds, err := link.Recv(); err != nil {
			return
		} else {
			control.CloseFDs(fds)
		}
		tok, err := seg.Export()
		if err != nil {
			return
		}
		payload, fds := tok.Encode()
		link.Send(frameWelcome, welcome{ringSize: rs, token: payload}.encode(), fds)
		tok.Close()
		link.Recv()
	}()
	c, err := (&Dialer{RingSize: rs}).Dial(path)
	if !errors.Is(err, ErrCorrupt) {
		if c != nil {
			c.Close()
		}
		t.Fatalf("Dial = %v; want ErrCorrupt", err)
	}
}

func TestDialFailsAWelcomeGrantingMoreThanItAskedFor(t *testing.T) {
	path := sockPath(t)
	cl, err := control.Listen(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	const asked, granted = 64 << 10, 1 << 20
	seg, err := shm.Create(int(segmentSize(granted)))
	if err != nil {
		t.Fatal(err)
	}
	defer seg.Close()
	mem, rs := seg.Bytes(), ring.Size(granted)
	for k := range 2 {
		if err := ring.Format(mem[k*rs:(k+1)*rs], granted); err != nil {
			t.Fatal(err)
		}
	}
	go func() {
		link, err := cl.Accept()
		if err != nil {
			return
		}
		defer link.Close()
		if _, _, fds, err := link.Recv(); err != nil {
			return
		} else {
			control.CloseFDs(fds)
		}
		tok, err := seg.Export()
		if err != nil {
			return
		}
		payload, fds := tok.Encode()
		link.Send(frameWelcome, welcome{ringSize: granted, token: payload}.encode(), fds)
		tok.Close()
		link.Recv()
	}()
	c, err := (&Dialer{RingSize: asked}).Dial(path)
	if !errors.Is(err, errProtocol) {
		if c != nil {
			t.Errorf("RingSize = %d", c.RingSize())
			c.Close()
		}
		t.Fatalf("Dial asking for %d bytes per ring = %v against a grant of %d; want the protocol violation", asked, err, granted)
	}
}

func TestAcceptAfterListenerCloseReturnsNilConnAndNetErrClosed(t *testing.T) {
	l, _ := listen(t, ListenConfig{})
	ch := make(chan error, 1)
	go func() {
		_, err := l.Accept()
		ch <- err
	}()
	time.Sleep(20 * time.Millisecond)
	l.Close()
	if err := waitErr(t, ch, time.Second); err != net.ErrClosed {
		t.Fatalf("Accept = %v; want net.ErrClosed", err)
	}
	var c net.Conn
	c, err := l.Accept()
	if c != nil || err != net.ErrClosed {
		t.Fatalf("Accept after Close = %v, %v; want a nil net.Conn, net.ErrClosed", c, err)
	}
}

func TestListenerDeadlineTimesOutAccept(t *testing.T) {
	l, _ := listen(t, ListenConfig{})
	l.SetDeadline(time.Now().Add(20 * time.Millisecond))
	if _, err := l.Accept(); !isTimeout(err) {
		t.Fatalf("Accept = %v; want os.ErrDeadlineExceeded as a net.Error timeout", err)
	}
	l.SetDeadline(time.Time{})
	acceptOne(t, l)
	c, err := Dial(l.Addr().String())
	if err != nil {
		t.Fatal("Dial after clearing the deadline:", err)
	}
	c.Close()
}

// serve makes n connections to l, accepting each before dialing the next, so
// that Accept returns the one just dialed, and returns their ends paired.
func serve(t *testing.T, l *Listener, path string, n int) (served, dialed []*Conn) {
	t.Helper()
	l.SetDeadline(time.Now().Add(10 * time.Second))
	for range n {
		dc, err := (&Dialer{Timeout: 10 * time.Second}).Dial(path)
		if err != nil {
			t.Fatal("Dial:", err)
		}
		t.Cleanup(func() { dc.Close() })
		c, err := l.Accept()
		if err != nil {
			t.Fatal("Accept:", err)
		}
		t.Cleanup(func() { c.Close() })
		served, dialed = append(served, c.(*Conn)), append(dialed, dc)
	}
	return served, dialed
}

func TestListenerServesConcurrentConnectionsIndependently(t *testing.T) {
	l, path := listen(t, ListenConfig{})
	served, dialed := serve(t, l, path, 4)
	buf := make([]byte, 8)
	for i := range 4 {
		if _, err := dialed[i].Write([]byte{byte(i)}); err != nil {
			t.Fatal(err)
		}
	}
	for i := range 4 {
		if n, err := served[i].Read(buf); err != nil || n != 1 || buf[0] != byte(i) {
			t.Fatalf("connection %d read %q, %v; want its own byte %d", i, buf[:n], err, i)
		}
	}
	dialed[0].Close()
	if _, err := served[0].Read(buf); err != io.EOF {
		t.Fatalf("Read on the closed connection = %v; want io.EOF", err)
	}
	for i := 1; i < 4; i++ {
		if _, err := served[i].Write([]byte("still")); err != nil {
			t.Fatalf("connection %d Write after another closed: %v", i, err)
		}
		if n, err := dialed[i].Read(buf); err != nil || string(buf[:n]) != "still" {
			t.Fatalf("connection %d Read after another closed = %q, %v", i, buf[:n], err)
		}
	}
}

func TestCloseLeaksNoGoroutines(t *testing.T) {
	before := runtime.NumGoroutine()
	func() {
		l, path := listen(t, ListenConfig{})
		served, dialed := serve(t, l, path, 2)
		blocked := make(chan error, 1)
		go func() {
			_, err := dialed[1].Next()
			blocked <- err
		}()
		if _, err := served[0].Write([]byte("x")); err != nil {
			t.Fatal(err)
		}
		if _, err := dialed[0].Read(make([]byte, 1)); err != nil {
			t.Fatal(err)
		}
		for _, c := range append(served, dialed...) {
			c.Close()
		}
		<-blocked
		// Left to Close: clients silent in their handshakes, and connections
		// waiting for Accept, dialed after them so that those handshakes began.
		for range 2 {
			silentClient(t, path)
		}
		queued := dialN(t, path, 2)
		l.Close()
		for _, c := range queued {
			c.Close()
		}
	}()
	deadline := time.Now().Add(2 * time.Second)
	for runtime.NumGoroutine() > before && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if n := runtime.NumGoroutine(); n > before {
		buf := make([]byte, 1<<16)
		t.Fatalf("%d goroutines after Close; %d before\n%s", n, before, buf[:runtime.Stack(buf, true)])
	}
}

func TestHelloRoundTrip(t *testing.T) {
	h := hello{version: protoVersion, ringSize: 1 << 20}
	got, err := decodeHello(h.encode())
	if err != nil || got != h {
		t.Fatalf("decodeHello = %+v, %v; want %+v", got, err, h)
	}
	w := welcome{ringSize: 64 << 10, token: []byte("tok")}
	gw, err := decodeWelcome(w.encode())
	if err != nil || gw.ringSize != w.ringSize || !bytes.Equal(gw.token, w.token) {
		t.Fatalf("decodeWelcome = %+v, %v; want %+v", gw, err, w)
	}
}
