package ipc

import (
	"errors"
	"fmt"
	"io"
	"net"
	"runtime"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	"github.com/wzshiming/ipc/internal/control"
	"github.com/wzshiming/ipc/internal/ring"
	"github.com/wzshiming/ipc/internal/shm"
)

// rawPeer returns the listener's end of a connection whose peer the test
// plays by hand: the peer's bare socket, which nothing reads unless the test
// does, and the peer's own mapping of the segment, which outlives the Conn's.
func rawPeer(t *testing.T, ringSize int) (c *Conn, sock *net.UnixConn, mem []byte) {
	t.Helper()
	path := sockPath(t)
	cl, err := control.Listen(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	accepted := make(chan *control.Link, 1)
	go func() {
		link, _ := cl.Accept()
		accepted <- link
	}()
	sock, err = net.DialUnix("unix", nil, &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { sock.Close() })
	link := <-accepted
	if link == nil {
		t.Fatal("Accept failed")
	}
	seg, err := shm.Create(int(segmentSize(uint64(ringSize))))
	if err != nil {
		link.Close()
		t.Fatal(err)
	}
	rs := ring.Size(ringSize)
	for k := range 2 {
		if err := ring.Format(seg.Bytes()[k*rs:(k+1)*rs], ringSize); err != nil {
			link.Close()
			seg.Close()
			t.Fatal(err)
		}
	}
	tok, err := seg.Export()
	if err != nil {
		link.Close()
		seg.Close()
		t.Fatal(err)
	}
	peerSeg, err := shm.Attach(tok)
	if err != nil {
		link.Close()
		seg.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { peerSeg.Close() })
	c, err = newConn(link, seg, ringSize, -1, path, true)
	if err != nil {
		link.Close()
		seg.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return c, sock, peerSeg.Bytes()
}

// consumerParked returns the word of mem, a connection's segment, in which
// the consumer of ring dir marks itself parked: offset 384 of its header.
func consumerParked(mem []byte, ringSize int, dir uint8) *uint32 {
	return (*uint32)(unsafe.Pointer(&mem[int(dir)*ring.Size(ringSize)+384]))
}

// dialPair makes one connection to l with d and returns its two ends, which
// close when the test ends.
func dialPair(t *testing.T, l *Listener, path string, d Dialer) (lc, dc *Conn) {
	t.Helper()
	accepted := make(chan net.Conn, 1)
	go func() {
		c, _ := l.Accept()
		accepted <- c
	}()
	d.Timeout = 10 * time.Second
	dc, err := d.Dial(path)
	if err != nil {
		t.Fatal("Dial:", err)
	}
	t.Cleanup(func() { dc.Close() })
	c := <-accepted
	if c == nil {
		t.Fatal("Accept failed")
	}
	lc = c.(*Conn)
	t.Cleanup(func() { lc.Close() })
	return lc, dc
}

func TestSendsFailAsAProtocolViolationOnceThePeerStopsReadingItsDoorbells(t *testing.T) {
	type result struct {
		sent    int64
		longest time.Duration // of a single call
		err     error
	}
	for name, send := range map[string]func(*Conn) result{
		"Write": func(c *Conn) (r result) {
			msg := make([]byte, 64)
			for {
				start := time.Now()
				n, err := c.Write(msg)
				r.longest = max(r.longest, time.Since(start))
				r.sent += int64(n)
				if err != nil {
					r.err = err
					return r
				}
			}
		},
		"ReadFrom": func(c *Conn) (r result) {
			start := time.Now()
			r.sent, r.err = c.ReadFrom(capReader(64))
			r.longest = time.Since(start)
			return r
		},
	} {
		t.Run(name, func(t *testing.T) {
			const ringSize = 64 << 10
			c, sock, mem := rawPeer(t, ringSize)
			// The peer drains the ring c sends on and parks after each record,
			// so that every commit rings, but never reads its socket. It still
			// rings c when c waits for room.
			cons, err := ring.NewConsumer(mem[:ring.Size(ringSize)])
			if err != nil {
				t.Fatal(err)
			}
			bell := []byte{frameDoorbell, 1, 0, 0, 0, dirListenerToDialer}
			var drained int64
			stop, stopped := make(chan struct{}), make(chan struct{})
			go func() {
				defer close(stopped)
				for {
					select {
					case <-stop:
						return
					default:
					}
					if rec, err := cons.TryNext(); err == nil {
						drained += int64(len(rec))
						if cons.Release() {
							sock.Write(bell)
						}
					} else {
						cons.Park()
						runtime.Gosched()
					}
				}
			}()
			stopPeer := func() {
				select {
				case <-stop:
				default:
					close(stop)
				}
				<-stopped
			}
			t.Cleanup(stopPeer)

			start := time.Now()
			c.SetWriteDeadline(start.Add(time.Second))
			done := make(chan result, 1)
			go func() { done <- send(c) }()
			var r result
			select {
			case r = <-done:
				if d := time.Since(start); d > 1500*time.Millisecond {
					t.Errorf("%s ended %v after the start; want within 1.5s", name, d)
				}
			case <-time.After(1500 * time.Millisecond):
				t.Errorf("%s still blocked 1.5s after the start, past its 1s write deadline", name)
				c.Close()
				r = <-done
			}
			if !errors.Is(r.err, errProtocol) {
				t.Errorf("%s = %v; want the protocol violation, neither a time-out nor ErrPeerClosed", name, r.err)
			}
			if err := c.Err(); !errors.Is(err, errProtocol) {
				t.Errorf("Err = %v; want the protocol violation", err)
			}
			if r.longest >= time.Second {
				t.Errorf("a single %s call took %v, outlasting its 1s write deadline", name, r.longest)
			}
			stopPeer()
			for {
				rec, err := cons.TryNext()
				if err != nil {
					break
				}
				drained += int64(len(rec))
				cons.Release()
			}
			if r.sent != drained {
				t.Errorf("%s counted %d bytes sent; the peer drained %d", name, r.sent, drained)
			}
		})
	}
}

func TestReadIsEOFAfterTheGracefulPeerCloseAFailedDoorbellNoticedFirst(t *testing.T) {
	for _, procs := range []int{1, runtime.GOMAXPROCS(0)} {
		t.Run(fmt.Sprintf("GOMAXPROCS=%d", procs), func(t *testing.T) {
			defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(procs))
			l, path := listen(t, ListenConfig{RingSize: 64 << 10, Spin: -1})
			const rounds = 200
			wrong := 0
			for i := range rounds {
				lc, dc := dialPair(t, l, path, Dialer{RingSize: 64 << 10, Spin: -1})
				go lc.Next()
				parked := consumerParked(lc.seg.Bytes(), lc.RingSize(), dirDialerToListener)
				for atomic.LoadUint32(parked) == 0 {
					runtime.Gosched()
				}
				// The peer closes with its reader parked, so this end's Write
				// rings a doorbell to a closed socket unless the goodbye is read first.
				lc.Close()
				_, werr := dc.Write([]byte("x"))
				dc.SetReadDeadline(time.Now().Add(5 * time.Second))
				_, rerr := dc.Read(make([]byte, 8))
				_, aerr := dc.Write([]byte("y"))
				if werr != nil && !errors.Is(werr, ErrPeerClosed) || rerr != io.EOF || !errors.Is(aerr, ErrPeerClosed) {
					if wrong++; wrong <= 3 {
						t.Errorf("round %d: Write = %v, Read = %v, Write = %v; want nil or ErrPeerClosed, io.EOF, ErrPeerClosed", i, werr, rerr, aerr)
					}
				}
				dc.Close()
			}
			if wrong > 0 {
				t.Errorf("%d of %d rounds wrong", wrong, rounds)
			}
		})
	}
}

func TestReadIsEOFAfterTheGracefulPeerCloseDuringContinuousSends(t *testing.T) {
	l, path := listen(t, ListenConfig{RingSize: 64 << 10, Spin: -1})
	const rounds = 300
	wrong := 0
	for i := range rounds {
		lc, dc := dialPair(t, l, path, Dialer{RingSize: 64 << 10, Spin: -1})
		released := make(chan struct{})
		go func() { // the peer drains, parking whenever its ring is empty
			for k := 1; ; k++ {
				if _, err := dc.Next(); err != nil {
					return
				}
				dc.Release()
				if k == 1+i%64 {
					close(released)
				}
			}
		}()
		sent := make(chan error, 1)
		go func() {
			msg := make([]byte, 8)
			for {
				if _, err := lc.Write(msg); err != nil {
					sent <- err
					return
				}
			}
		}()
		select {
		case <-released:
		case err := <-sent:
			t.Fatalf("round %d: Write before the peer's close = %v", i, err)
		}
		dc.Close()
		werr := waitErr(t, sent, 5*time.Second)
		lc.SetReadDeadline(time.Now().Add(5 * time.Second))
		_, rerr := lc.Read(make([]byte, 8))
		if !errors.Is(werr, ErrPeerClosed) || rerr != io.EOF {
			if wrong++; wrong <= 3 {
				t.Errorf("round %d: Write = %v, then Read = %v; want ErrPeerClosed, then io.EOF", i, werr, rerr)
			}
		}
		lc.Close()
	}
	if wrong > 0 {
		t.Errorf("%d of %d rounds wrong", wrong, rounds)
	}
}

// halfClosedPeer returns a Conn whose peer shut only its receiving side, so
// that a doorbell fails while the link stays open, after such a doorbell.
// It skips the test where that doorbell does not fail.
func halfClosedPeer(t *testing.T) *Conn {
	t.Helper()
	const ringSize = 64 << 10
	c, sock, mem := rawPeer(t, ringSize)
	atomic.StoreUint32(consumerParked(mem, ringSize, dirListenerToDialer), 1)
	if err := sock.CloseRead(); err != nil {
		t.Fatal(err)
	}
	c.Write([]byte("x")) // its doorbell fails
	select {
	case <-c.Done():
	case <-time.After(time.Second):
		t.Skipf("on %s a doorbell to a peer that shut its receiving side does not fail", runtime.GOOS)
	}
	return c
}

func TestReceiveWaitsForWhyThePeerLeftNoLongerThanASecondItsDeadlineOrClose(t *testing.T) {
	const slack = 500 * time.Millisecond
	t.Run("a second", func(t *testing.T) {
		c := halfClosedPeer(t)
		start := time.Now()
		_, err := c.Read(make([]byte, 8))
		if d := time.Since(start); !errors.Is(err, ErrPeerClosed) || d > time.Second+slack {
			t.Fatalf("Read = %v after %v; want ErrPeerClosed within 1s", err, d)
		}
	})
	t.Run("deadline", func(t *testing.T) {
		c := halfClosedPeer(t)
		start := time.Now()
		c.SetReadDeadline(start.Add(100 * time.Millisecond))
		_, err := c.Read(make([]byte, 8))
		if d := time.Since(start); !isTimeout(err) || d > 100*time.Millisecond+slack {
			t.Fatalf("Read = %v after %v; want os.ErrDeadlineExceeded at its 100ms deadline", err, d)
		}
	})
	t.Run("Close", func(t *testing.T) {
		c := halfClosedPeer(t)
		start := time.Now()
		time.AfterFunc(100*time.Millisecond, func() { c.Close() })
		_, err := c.Read(make([]byte, 8))
		if d := time.Since(start); err != net.ErrClosed || d > 100*time.Millisecond+slack {
			t.Fatalf("Read = %v after %v; want net.ErrClosed at the Close 100ms in", err, d)
		}
	})
}

func TestCommitAfterLocalClosePublishesNothing(t *testing.T) {
	lc, dc := pair(t, ListenConfig{}, Dialer{})
	p, err := dc.TryReserve(5)
	if err != nil {
		t.Fatal(err)
	}
	copy(p, "ghost")
	dc.Close()
	if err := dc.Commit(); err != net.ErrClosed {
		t.Fatalf("Commit after Close = %v; want net.ErrClosed", err)
	}
	<-lc.Done()
	if rec, err := lc.TryNext(); err != io.EOF {
		t.Fatalf("the peer's TryNext = %q, %v; want io.EOF, no record", rec, err)
	}
}
