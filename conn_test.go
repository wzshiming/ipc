package ipc

import (
	"bytes"
	"context"
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

func listen(t testing.TB, cfg Config) (*Listener, string) {
	path := sockPath(t)
	l, err := Listen(path, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	return l, path
}

// pair returns the listener-side and dialer-side ends of one connection.
func pair(t testing.TB, lcfg, dcfg Config) (lc, dc *Conn) {
	l, path := listen(t, lcfg)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	type result struct {
		c   *Conn
		err error
	}
	ch := make(chan result, 1)
	go func() {
		c, err := l.Accept(ctx)
		ch <- result{c, err}
	}()
	dc, err := Dial(ctx, path, dcfg)
	if err != nil {
		t.Fatal("Dial:", err)
	}
	r := <-ch
	if r.err != nil {
		dc.Close()
		t.Fatal("Accept:", r.err)
	}
	t.Cleanup(func() {
		r.c.Close()
		dc.Close()
	})
	return r.c, dc
}

func TestDialerRequestIsClampedToListenerLimits(t *testing.T) {
	lc, dc := pair(t, Config{Lanes: 2, RingSize: 128 << 10}, Config{Lanes: 4, RingSize: 1 << 20})
	for _, c := range []*Conn{lc, dc} {
		if c.Lanes() != 2 || c.RingSize() != 128<<10 {
			t.Fatalf("Lanes, RingSize = %d, %d; want 2, %d", c.Lanes(), c.RingSize(), 128<<10)
		}
		if c.MaxRecord() != ring.MaxRecord(128<<10) {
			t.Fatalf("MaxRecord = %d; want %d", c.MaxRecord(), ring.MaxRecord(128<<10))
		}
	}
}

func TestZeroConfigUsesDefaults(t *testing.T) {
	lc, dc := pair(t, Config{Lanes: 8}, Config{})
	for _, c := range []*Conn{lc, dc} {
		if c.Lanes() != 1 || c.RingSize() != 4<<20 {
			t.Fatalf("Lanes, RingSize = %d, %d; want 1, %d", c.Lanes(), c.RingSize(), 4<<20)
		}
	}
}

func TestListenRejectsInvalidConfig(t *testing.T) {
	for _, cfg := range []Config{
		{Lanes: -1},
		{Lanes: 1025},
		{RingSize: 100000},
		{RingSize: 32 << 10},
		{Lanes: 1024, RingSize: 1 << 30},
	} {
		l, err := Listen(sockPath(t), cfg)
		if err == nil {
			l.Close()
			t.Errorf("Listen(%+v) succeeded", cfg)
		}
	}
}

func TestAddrsAreIPCPaths(t *testing.T) {
	l, path := listen(t, Config{})
	if a := l.Addr(); a.Network() != "ipc" || a.String() != path {
		t.Fatalf("Listener.Addr = %s:%s", a.Network(), a.String())
	}
	ctx := context.Background()
	go l.Accept(ctx)
	dc, err := Dial(ctx, path, Config{})
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

func TestCancelledNextReturnsCtxErrAndKeepsData(t *testing.T) {
	lc, dc := pair(t, Config{}, Config{Spin: -1})
	ctx, cancel := context.WithCancel(context.Background())
	ch := make(chan error, 1)
	go func() {
		_, err := dc.Lane(0).Next(ctx)
		ch <- err
	}()
	time.Sleep(50 * time.Millisecond)
	cancel()
	if err := waitErr(t, ch, 100*time.Millisecond); !errors.Is(err, context.Canceled) {
		t.Fatalf("Next after cancel = %v; want context.Canceled", err)
	}
	bg := context.Background()
	if err := lc.Lane(0).Send(bg, []byte("after")); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 16)
	n, err := dc.Lane(0).Recv(bg, buf)
	if err != nil || string(buf[:n]) != "after" {
		t.Fatalf("Recv = %q, %v; want after", buf[:n], err)
	}
}

func TestCancelledReserveReturnsCtxErrAndKeepsData(t *testing.T) {
	lc, dc := pair(t, Config{RingSize: 64 << 10}, Config{RingSize: 64 << 10, Spin: -1})
	tx, rx := dc.Lane(0), lc.Lane(0)
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
	ctx, cancel := context.WithCancel(context.Background())
	ch := make(chan error, 1)
	go func() {
		_, err := tx.Reserve(ctx, 1000)
		ch <- err
	}()
	time.Sleep(50 * time.Millisecond)
	cancel()
	if err := waitErr(t, ch, 100*time.Millisecond); !errors.Is(err, context.Canceled) {
		t.Fatalf("Reserve after cancel = %v; want context.Canceled", err)
	}
	bg := context.Background()
	p := make([]byte, 1000)
	putSeq(p, sent)
	go func() { ch <- tx.Send(bg, p) }()
	for want := uint64(0); want <= sent; want++ {
		rec, err := rx.Next(bg)
		if err != nil {
			t.Fatal(err)
		}
		if got := getSeq(rec); got != want {
			t.Fatalf("record seq %d; want %d", got, want)
		}
		rx.Release()
	}
	if err := waitErr(t, ch, time.Second); err != nil {
		t.Fatal("Send after cancel:", err)
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
	lc, dc := pair(t, Config{}, Config{Spin: -1})
	ch := make(chan error, 1)
	go func() {
		_, err := dc.Lane(0).Next(context.Background())
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
	lc, dc := pair(t, Config{RingSize: 64 << 10}, Config{RingSize: 64 << 10, Spin: -1})
	tx := dc.Lane(0)
	for {
		if _, err := tx.TryReserve(4000); err != nil {
			break
		}
		tx.Commit()
	}
	ch := make(chan error, 1)
	go func() {
		_, err := tx.Reserve(context.Background(), 4000)
		ch <- err
	}()
	time.Sleep(20 * time.Millisecond)
	lc.Close()
	if err := waitErr(t, ch, time.Second); !errors.Is(err, ErrPeerClosed) {
		t.Fatalf("Reserve = %v; want ErrPeerClosed", err)
	}
}

// commitSeq commits n records of size bytes numbered 0..n-1 on l.
func commitSeq(t *testing.T, l *Lane, n, size int) {
	t.Helper()
	for i := range n {
		p, err := l.TryReserve(size)
		if err != nil {
			t.Fatalf("TryReserve record %d: %v", i, err)
		}
		putSeq(p, uint64(i))
		if err := l.Commit(); err != nil {
			t.Fatal(err)
		}
	}
}

// drainSeq reads n records numbered 0..n-1 from l, alternating TryNext and
// Next, then expects end from both.
func drainSeq(t *testing.T, l *Lane, n int, end error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for i := range n {
		var rec []byte
		var err error
		if i%2 == 0 {
			rec, err = l.TryNext()
		} else {
			rec, err = l.Next(ctx)
		}
		if err != nil {
			t.Fatalf("record %d after peer close: %v", i, err)
		}
		if got := getSeq(rec); got != uint64(i) {
			t.Fatalf("record seq %d; want %d", got, i)
		}
		if err := l.Release(); err != nil {
			t.Fatalf("Release record %d after peer close: %v", i, err)
		}
	}
	if _, err := l.Next(ctx); err != end {
		t.Fatalf("Next on drained lane = %v; want %v", err, end)
	}
	if _, err := l.TryNext(); err != end {
		t.Fatalf("TryNext on drained lane = %v; want %v", err, end)
	}
}

func TestPeerCloseLeavesCommittedRecordsDrainableThenEOF(t *testing.T) {
	lc, dc := pair(t, Config{}, Config{})
	commitSeq(t, lc.Lane(0), 1000, 1<<10)
	lc.Close()
	<-dc.Done()
	drainSeq(t, dc.Lane(0), 1000, io.EOF)
	if !errors.Is(dc.Err(), ErrPeerClosed) {
		t.Fatalf("Err = %v; want ErrPeerClosed", dc.Err())
	}
}

func TestParkedNextWokenByPeerCloseReturnsRecordCommittedBeforeIt(t *testing.T) {
	lc, dc := pair(t, Config{}, Config{Spin: -1})
	type result struct {
		seq uint64
		err error
	}
	ch := make(chan result, 1)
	go func() {
		rec, err := dc.Lane(0).Next(context.Background())
		if err != nil {
			ch <- result{err: err}
			return
		}
		ch <- result{seq: getSeq(rec)}
		dc.Lane(0).Release()
	}()
	time.Sleep(20 * time.Millisecond)
	// Commit without ringing, so only the peer's close wakes the parked Next.
	prod := lc.Lane(0).prod
	p, err := prod.TryReserve(8)
	if err != nil {
		t.Fatal(err)
	}
	putSeq(p, 7)
	prod.Commit()
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
	lc, dc := pair(t, Config{}, Config{})
	commitSeq(t, lc.Lane(0), 1000, 1<<10)
	lc.link.Close() // vanish without Bye
	<-dc.Done()
	drainSeq(t, dc.Lane(0), 1000, ErrPeerClosed)
	if !errors.Is(dc.Err(), ErrPeerClosed) {
		t.Fatalf("Err = %v; want ErrPeerClosed", dc.Err())
	}
}

func TestReleaseAfterPeerCloseSucceedsWhenPeerWasParked(t *testing.T) {
	lc, dc := pair(t, Config{RingSize: 64 << 10, Spin: -1}, Config{RingSize: 64 << 10})
	tx := lc.Lane(0)
	n := 0
	for ; ; n++ {
		p, err := tx.TryReserve(4000)
		if err != nil {
			break
		}
		putSeq(p, uint64(n))
		tx.Commit()
	}
	ch := make(chan error, 1)
	go func() {
		_, err := tx.Reserve(context.Background(), 4000)
		ch <- err
	}()
	time.Sleep(20 * time.Millisecond)
	lc.Close()
	waitErr(t, ch, time.Second)
	<-dc.Done()
	drainSeq(t, dc.Lane(0), n, io.EOF)
}

func TestPeerCloseFailsReserveTryReserveAndSendAtOnce(t *testing.T) {
	lc, dc := pair(t, Config{}, Config{})
	commitSeq(t, lc.Lane(0), 10, 100)
	lc.Close()
	<-dc.Done()
	l := dc.Lane(0)
	if _, err := l.TryReserve(8); !errors.Is(err, ErrPeerClosed) {
		t.Fatalf("TryReserve = %v; want ErrPeerClosed", err)
	}
	if _, err := l.Reserve(context.Background(), 8); !errors.Is(err, ErrPeerClosed) {
		t.Fatalf("Reserve = %v; want ErrPeerClosed", err)
	}
	if err := l.Send(context.Background(), []byte("x")); !errors.Is(err, ErrPeerClosed) {
		t.Fatalf("Send = %v; want ErrPeerClosed", err)
	}
}

func TestLocalCloseAfterPeerCloseUnmapsAndStopsDraining(t *testing.T) {
	lc, dc := pair(t, Config{}, Config{})
	commitSeq(t, lc.Lane(0), 10, 100)
	lc.Close()
	<-dc.readerDone // the peer-close shutdown has finished
	if dc.unmapped.Load() {
		t.Fatal("segment unmapped by peer close; draining needs it until local Close")
	}
	dc.Close()
	if !dc.unmapped.Load() {
		t.Fatal("segment still mapped after local Close")
	}
	if _, err := dc.Lane(0).TryNext(); err != net.ErrClosed {
		t.Fatalf("TryNext after local Close = %v; want net.ErrClosed", err)
	}
	if _, err := dc.Lane(0).TryReserve(8); err != net.ErrClosed {
		t.Fatalf("TryReserve after local Close = %v; want net.ErrClosed", err)
	}
	if err := dc.Err(); err != net.ErrClosed {
		t.Fatalf("Err after local Close = %v; want net.ErrClosed", err)
	}
}

func TestLocalCloseWakesBlockedNextWithNetErrClosed(t *testing.T) {
	_, dc := pair(t, Config{}, Config{Spin: -1})
	ch := make(chan error, 1)
	go func() {
		_, err := dc.Lane(0).Next(context.Background())
		ch <- err
	}()
	time.Sleep(20 * time.Millisecond)
	dc.Close()
	if err := waitErr(t, ch, time.Second); err != net.ErrClosed {
		t.Fatalf("Next = %v; want net.ErrClosed", err)
	}
	if _, err := dc.Lane(0).TryNext(); err != net.ErrClosed {
		t.Fatalf("TryNext after Close = %v; want net.ErrClosed", err)
	}
	if err := dc.Err(); err != net.ErrClosed {
		t.Fatalf("Err after Close = %v; want net.ErrClosed", err)
	}
}

func TestLocalCloseWakesBlockedReserveWithNetErrClosed(t *testing.T) {
	_, dc := pair(t, Config{RingSize: 64 << 10}, Config{RingSize: 64 << 10, Spin: -1})
	tx := dc.Lane(0)
	for {
		if _, err := tx.TryReserve(4000); err != nil {
			break
		}
		tx.Commit()
	}
	ch := make(chan error, 1)
	go func() {
		_, err := tx.Reserve(context.Background(), 4000)
		ch <- err
	}()
	time.Sleep(20 * time.Millisecond)
	dc.Close()
	if err := waitErr(t, ch, time.Second); err != net.ErrClosed {
		t.Fatalf("Reserve = %v; want net.ErrClosed", err)
	}
}

func TestCommitAfterLocalCloseIsNetErrClosed(t *testing.T) {
	_, dc := pair(t, Config{}, Config{})
	if _, err := dc.Lane(0).TryReserve(8); err != nil {
		t.Fatal(err)
	}
	dc.Close()
	if err := dc.Lane(0).Commit(); err != net.ErrClosed {
		t.Fatalf("Commit after Close = %v; want net.ErrClosed", err)
	}
}

func TestCloseDefersUnmapUntilInflightNextReleased(t *testing.T) {
	lc, dc := pair(t, Config{}, Config{})
	bg := context.Background()
	if err := lc.Lane(0).Send(bg, []byte("hello")); err != nil {
		t.Fatal(err)
	}
	rec, err := dc.Lane(0).Next(bg)
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
	dc.Lane(0).Release()
	if !dc.unmapped.Load() {
		t.Fatal("segment still mapped after the last slice was released")
	}
}

func TestCloseDefersUnmapUntilInflightReserveAborted(t *testing.T) {
	_, dc := pair(t, Config{}, Config{})
	p, err := dc.Lane(0).Reserve(context.Background(), 100)
	if err != nil {
		t.Fatal(err)
	}
	dc.Close()
	if dc.unmapped.Load() {
		t.Fatal("segment unmapped while a Reserve slice is outstanding")
	}
	clear(p)
	dc.Lane(0).Abort()
	if !dc.unmapped.Load() {
		t.Fatal("segment still mapped after the reservation was aborted")
	}
}

func TestCloseUnmapsAtOnceWithoutInflightSlices(t *testing.T) {
	lc, _ := pair(t, Config{}, Config{})
	lc.Close()
	if !lc.unmapped.Load() {
		t.Fatal("segment still mapped after Close with nothing outstanding")
	}
}

func rawHello(t *testing.T, path string, h hello) (typ uint8, payload []byte) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	link, err := control.Dial(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer link.Close()
	link.SetDeadline(time.Now().Add(5 * time.Second))
	if err := link.Send(frameHello, h.encode(), nil); err != nil {
		t.Fatal(err)
	}
	typ, payload, fds, err := link.Recv()
	if err != nil {
		t.Fatal(err)
	}
	discardFDs(fds)
	if _, _, _, err := link.Recv(); !errors.Is(err, io.EOF) {
		t.Fatalf("Recv after Reject = %v; want io.EOF (listener closes the link)", err)
	}
	return typ, payload
}

func TestListenerRejectsInvalidHello(t *testing.T) {
	l, path := listen(t, Config{})
	go l.Accept(context.Background())
	for name, h := range map[string]hello{
		"version":       {version: protoVersion + 1, lanes: 1, ringSize: 64 << 10},
		"lanes 0":       {version: protoVersion, lanes: 0, ringSize: 64 << 10},
		"lanes 1025":    {version: protoVersion, lanes: 1025, ringSize: 64 << 10},
		"ring not pow2": {version: protoVersion, lanes: 1, ringSize: 100000},
		"ring too big":  {version: protoVersion, lanes: 1, ringSize: 2 << 30},
	} {
		t.Run(name, func(t *testing.T) {
			if typ, reason := rawHello(t, path, h); typ != frameReject {
				t.Fatalf("frame type %d (%q); want Reject %d", typ, reason, frameReject)
			}
		})
	}
}

func TestDialReturnsErrRejected(t *testing.T) {
	l, path := listen(t, Config{})
	go l.Accept(context.Background())
	c, err := Dial(context.Background(), path, Config{RingSize: 100000})
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
	seg, err := shm.Create(int(segmentSize(1, rs)))
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
			discardFDs(fds)
		}
		tok, err := seg.Export()
		if err != nil {
			return
		}
		payload, fds := tok.Encode()
		link.Send(frameWelcome, welcome{lanes: 1, ringSize: rs, token: payload}.encode(), fds)
		tok.Close()
		link.Recv()
	}()
	c, err := Dial(context.Background(), path, Config{RingSize: rs})
	if !errors.Is(err, ErrCorrupt) {
		if c != nil {
			c.Close()
		}
		t.Fatalf("Dial = %v; want ErrCorrupt", err)
	}
}

func TestAcceptReturnsErrClosedAfterListenerClose(t *testing.T) {
	l, _ := listen(t, Config{})
	ch := make(chan error, 1)
	go func() {
		_, err := l.Accept(context.Background())
		ch <- err
	}()
	time.Sleep(20 * time.Millisecond)
	l.Close()
	if err := waitErr(t, ch, time.Second); err != net.ErrClosed {
		t.Fatalf("Accept = %v; want net.ErrClosed", err)
	}
}

func TestAcceptHonoursCtx(t *testing.T) {
	l, _ := listen(t, Config{})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := l.Accept(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Accept = %v; want context.DeadlineExceeded", err)
	}
}

func TestCloseLeaksNoGoroutines(t *testing.T) {
	before := runtime.NumGoroutine()
	func() {
		path := sockPath(t)
		l, err := Listen(path, Config{Lanes: 2})
		if err != nil {
			t.Fatal(err)
		}
		defer l.Close()
		ctx := context.Background()
		ch := make(chan *Conn, 1)
		go func() {
			c, _ := l.Accept(ctx)
			ch <- c
		}()
		dc, err := Dial(ctx, path, Config{Lanes: 2, Spin: -1})
		if err != nil {
			t.Fatal(err)
		}
		lc := <-ch
		if lc == nil {
			t.Fatal("Accept failed")
		}
		blocked := make(chan error, 1)
		go func() {
			_, err := dc.Lane(1).Next(ctx)
			blocked <- err
		}()
		if err := lc.Lane(0).Send(ctx, []byte("x")); err != nil {
			t.Fatal(err)
		}
		buf := make([]byte, 1)
		if _, err := dc.Lane(0).Recv(ctx, buf); err != nil {
			t.Fatal(err)
		}
		lc.Close()
		dc.Close()
		<-blocked
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
	h := hello{version: protoVersion, lanes: 3, ringSize: 1 << 20}
	got, err := decodeHello(h.encode())
	if err != nil || got != h {
		t.Fatalf("decodeHello = %+v, %v; want %+v", got, err, h)
	}
	w := welcome{lanes: 2, ringSize: 64 << 10, token: []byte("tok")}
	gw, err := decodeWelcome(w.encode())
	if err != nil || gw.lanes != w.lanes || gw.ringSize != w.ringSize || !bytes.Equal(gw.token, w.token) {
		t.Fatalf("decodeWelcome = %+v, %v; want %+v", gw, err, w)
	}
}
