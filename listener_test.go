package ipc

import (
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wzshiming/ipc/internal/control"
	"github.com/wzshiming/ipc/internal/shm"
)

// silentClient connects a control link to path and sends nothing, so that
// the listener's handshake for it waits for a Hello.
func silentClient(t *testing.T, path string) *control.Link {
	t.Helper()
	link, err := control.Dial(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { link.Close() })
	return link
}

// sendHello sends link's Hello and returns the frame type of the answer, or
// the error that ended the link instead.
func sendHello(link *control.Link) (uint8, error) {
	link.SetDeadline(time.Now().Add(handshakeTimeout))
	link.Send(frameHello, hello{version: protoVersion, ringSize: minRingSize}.encode(), nil)
	typ, _, fds, err := link.Recv()
	control.CloseFDs(fds)
	return typ, err
}

// dialN dials n connections to path one after another, with no Accept on
// the other end, and returns them.
func dialN(t *testing.T, path string, n int) []*Conn {
	t.Helper()
	conns := make([]*Conn, 0, n)
	for i := range n {
		c, err := (&Dialer{Timeout: 10 * time.Second}).Dial(path)
		if err != nil {
			t.Fatalf("Dial %d: %v", i, err)
		}
		t.Cleanup(func() { c.Close() })
		conns = append(conns, c)
	}
	return conns
}

// acceptN accepts n connections on l and returns them in Accept order.
func acceptN(t *testing.T, l *Listener, n int) []*Conn {
	t.Helper()
	l.SetDeadline(time.Now().Add(10 * time.Second))
	conns := make([]*Conn, 0, n)
	for i := range n {
		c, err := l.Accept()
		if err != nil {
			t.Fatalf("Accept %d: %v", i, err)
		}
		t.Cleanup(func() { c.Close() })
		conns = append(conns, c.(*Conn))
	}
	return conns
}

// acceptErr runs Accept in the background and delivers its error, or an
// error naming the connection it returned, which it closes.
func acceptErr(l *Listener) <-chan error {
	ch := make(chan error, 1)
	go func() {
		c, err := l.Accept()
		if c != nil {
			c.Close()
			err = errors.New("a connection")
		}
		ch <- err
	}()
	return ch
}

// waitAccept returns what acceptErr delivers, failing the test unless it
// arrives within d of what should have ended the Accept.
func waitAccept(t *testing.T, ch <-chan error, d time.Duration, after string) error {
	t.Helper()
	select {
	case err := <-ch:
		return err
	case <-time.After(d):
		t.Fatalf("Accept still blocked %v after %s", d, after)
		return nil
	}
}

// awaitHandshakes waits until n goroutines are inside a listener's handshake.
func awaitHandshakes(t *testing.T, n int) {
	t.Helper()
	await(t, "handshakes in flight", func() bool { return goroutines("", "ipc.(*Listener).handshake(") >= n })
}

// awaitAccept waits until a goroutine waits inside Listener.Accept.
func awaitAccept(t *testing.T) {
	t.Helper()
	await(t, "an Accept to wait", func() bool { return goroutines("select", "ipc.(*Listener).Accept(") > 0 })
}

func TestSilentClientsDelayNeitherADialNorItsAccept(t *testing.T) {
	l, path := listen(t, ListenConfig{})
	for range 8 {
		silentClient(t, path)
	}
	accepted := make(chan net.Conn, 1)
	go func() {
		c, _ := l.Accept()
		accepted <- c
	}()
	start := time.Now()
	dc, err := (&Dialer{Timeout: time.Second}).Dial(path)
	if err != nil {
		t.Fatalf("Dial beside 8 silent clients = %v after %v; want it done within 1s", err, time.Since(start))
	}
	defer dc.Close()
	var c net.Conn
	select {
	case c = <-accepted:
	case <-time.After(time.Minute):
		t.Fatal("Accept beside 8 silent clients still blocked a minute later")
	}
	if c == nil {
		t.Fatal("Accept beside 8 silent clients failed")
	}
	defer c.Close()
	if d := time.Since(start); d > time.Second {
		t.Fatalf("Dial and Accept beside 8 silent clients took %v; want at most 1s", d)
	}
	if _, err := dc.Write([]byte("x")); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 8)
	if n, err := c.Read(buf); err != nil || string(buf[:n]) != "x" {
		t.Fatalf("the accepted connection read %q, %v; want what its dialer wrote", buf[:n], err)
	}
}

func TestSixteenConcurrentDialsEachCarryTheirOwnData(t *testing.T) {
	l, path := listen(t, ListenConfig{})
	const n = 16
	dialed := make([]*Conn, n)
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := range n {
		wg.Go(func() {
			dialed[i], errs[i] = (&Dialer{Timeout: 10 * time.Second}).Dial(path)
		})
	}
	wg.Wait()
	for _, c := range dialed {
		if c != nil {
			t.Cleanup(func() { c.Close() })
		}
	}
	for i, err := range errs {
		if err != nil {
			t.Fatalf("Dial %d of %d at once: %v", i, n, err)
		}
	}
	// Each dialer sends its index, the listener's end echoes what it reads,
	// and each dialer must read its own index back.
	for i, c := range dialed {
		if _, err := c.Write([]byte{byte(i)}); err != nil {
			t.Fatal(err)
		}
	}
	buf := make([]byte, 8)
	for _, c := range acceptN(t, l, n) {
		k, err := c.Read(buf)
		if err != nil || k != 1 {
			t.Fatalf("Read on an accepted connection = %d bytes, %v; want 1", k, err)
		}
		if _, err := c.Write(buf[:1]); err != nil {
			t.Fatal(err)
		}
	}
	for i, c := range dialed {
		k, err := c.Read(buf)
		if err != nil || k != 1 || buf[0] != byte(i) {
			t.Fatalf("dialer %d read %v, %v; want its own index echoed", i, buf[:k], err)
		}
	}
}

func TestDialBeyondTheBacklogCompletesOnceAnAcceptFreesASlot(t *testing.T) {
	l, path := listen(t, ListenConfig{})
	dialN(t, path, backlog)
	type result struct {
		c   *Conn
		err error
	}
	ch := make(chan result, 1)
	go func() {
		c, err := (&Dialer{Timeout: 10 * time.Second}).Dial(path)
		ch <- result{c, err}
	}()
	select {
	case r := <-ch:
		if r.c != nil {
			r.c.Close()
		}
		t.Fatalf("Dial %d with %d connections waiting for Accept returned %v; want it to wait", backlog+1, backlog, r.err)
	case <-time.After(200 * time.Millisecond):
	}
	acceptN(t, l, 1)
	select {
	case r := <-ch:
		if r.err != nil {
			t.Fatal("Dial after an Accept freed a slot:", r.err)
		}
		r.c.Close()
	case <-time.After(time.Minute):
		t.Fatal("Dial still waiting a minute after an Accept freed a slot")
	}
}

func TestCloseWakesAnAcceptWaitingBesideAHandshakeInFlight(t *testing.T) {
	l, path := listen(t, ListenConfig{})
	silentClient(t, path)
	accepted := acceptErr(l)
	awaitHandshakes(t, 1)
	awaitAccept(t)
	closed := make(chan error, 1)
	go func() { closed <- l.Close() }()
	if err := waitAccept(t, accepted, time.Second, "Close"); err != net.ErrClosed {
		t.Fatalf("Accept beside a handshake in flight = %v after Close; want net.ErrClosed", err)
	}
	if err := waitErr(t, closed, time.Second); err != nil {
		t.Fatal("Close:", err)
	}
}

func TestAHelloSentAfterCloseYieldsNoConnection(t *testing.T) {
	l, path := listen(t, ListenConfig{})
	link := silentClient(t, path)
	accepted := acceptErr(l)
	awaitHandshakes(t, 1)
	awaitAccept(t)
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	if typ, err := sendHello(link); err == nil {
		t.Errorf("a Hello sent after Close was answered with frame %d; want its link ended", typ)
	}
	if err := waitAccept(t, accepted, handshakeTimeout, "Close"); err != net.ErrClosed {
		t.Errorf("the Accept waiting across Close = %v; want net.ErrClosed", err)
	}
	if c, err := l.Accept(); c != nil || err != net.ErrClosed {
		t.Errorf("Accept after Close = %v, %v; want no connection, net.ErrClosed", c, err)
	}
}

func TestCloseEndsTheConnectionsWaitingForAccept(t *testing.T) {
	l, path := listen(t, ListenConfig{})
	dc := dialN(t, path, 1)[0]
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-dc.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("the dialer of a connection waiting for Accept saw no end after Close")
	}
	if _, err := dc.Next(); err != io.EOF {
		t.Fatalf("Next after the listener's Close = %v; want io.EOF", err)
	}
}

func TestAPastDeadlineWakesAcceptButNotTheHandshakeBesideIt(t *testing.T) {
	l, path := listen(t, ListenConfig{})
	link := silentClient(t, path)
	accepted := acceptErr(l)
	awaitHandshakes(t, 1)
	awaitAccept(t)
	l.SetDeadline(time.Unix(1, 0))
	if err := waitAccept(t, accepted, time.Second, "its deadline passed"); !isTimeout(err) {
		t.Fatalf("Accept beside a handshake in flight = %v once the deadline passed; want os.ErrDeadlineExceeded", err)
	}
	if typ, err := sendHello(link); err != nil || typ != frameWelcome {
		t.Fatalf("a Hello sent past the deadline was answered with frame %d, %v; want a Welcome", typ, err)
	}
	if c, err := l.Accept(); c != nil || !isTimeout(err) {
		if c != nil {
			c.Close()
		}
		t.Fatalf("Accept past the deadline = %v, %v; want no connection, os.ErrDeadlineExceeded", c, err)
	}
	l.SetDeadline(time.Now().Add(10 * time.Second))
	c, err := l.Accept()
	if err != nil {
		t.Fatal("Accept once the deadline moved on:", err)
	}
	c.Close()
}

// unresponsiveDialer completes a handshake on a bare link to path and then
// neither reads nor closes it.
func unresponsiveDialer(t *testing.T, path string) {
	t.Helper()
	if typ, err := sendHello(silentClient(t, path)); err != nil || typ != frameWelcome {
		t.Fatalf("handshake ended with frame %d, %v; want a Welcome", typ, err)
	}
}

func TestCloseEndsSixteenConnectionsOfUnresponsiveDialersWithinASecond(t *testing.T) {
	l, path := listen(t, ListenConfig{})
	for range 16 {
		unresponsiveDialer(t, path)
	}
	start := time.Now()
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	if d := time.Since(start); d > time.Second {
		t.Fatalf("Close took %v with 16 connections of unresponsive dialers waiting for Accept; want at most 1s", d)
	}
}

func TestAcceptReturnsALocalFailureOnceAndTheListenerServesOn(t *testing.T) {
	errLocal := errors.New("injected: no segment")
	create := createSegment
	var failed atomic.Bool
	createSegment = func(size int) (*shm.Segment, error) {
		if failed.CompareAndSwap(false, true) {
			return nil, errLocal
		}
		return create(size)
	}
	t.Cleanup(func() { createSegment = create })
	l, path := listen(t, ListenConfig{})
	if c, err := Dial(path); !errors.Is(err, ErrRejected) {
		if c != nil {
			c.Close()
		}
		t.Fatalf("Dial while the listener cannot create a segment = %v; want ErrRejected", err)
	}
	l.SetDeadline(time.Now().Add(10 * time.Second))
	if c, err := l.Accept(); err != errLocal {
		if c != nil {
			c.Close()
		}
		t.Fatalf("Accept after the listener failed to create a segment = %v, %v; want that failure", c, err)
	}
	lc, dc := dialPair(t, l, path, Dialer{})
	if _, err := dc.Write([]byte("x")); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 8)
	if n, err := lc.Read(buf); err != nil || string(buf[:n]) != "x" {
		t.Fatalf("the connection after the failure read %q, %v; want what its dialer wrote", buf[:n], err)
	}
}
