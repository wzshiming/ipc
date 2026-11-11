package ipc

import (
	"errors"
	"io"
	"net"
	"runtime"
	"testing"
	"time"

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

func TestSendsFailAsAProtocolViolationOnceThePeerStopsReadingItsDoorbells(t *testing.T) {
	type result struct {
		sent int64
		err  error
	}
	for name, send := range map[string]func(*Conn) result{
		"Write": func(c *Conn) (r result) {
			msg := make([]byte, 64)
			for {
				n, err := c.Write(msg)
				r.sent += int64(n)
				if err != nil {
					r.err = err
					return r
				}
			}
		},
		"ReadFrom": func(c *Conn) (r result) {
			r.sent, r.err = c.ReadFrom(capReader(64))
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

			// No deadline: however long load delays the doorbells that fill the
			// peer's socket, only the protocol violation may end the sends.
			const hang = time.Minute
			done := make(chan result, 1)
			go func() { done <- send(c) }()
			var r result
			select {
			case r = <-done:
			case <-time.After(hang):
				t.Errorf("%s still blocked after %v; want it ended by the protocol violation", name, hang)
				c.Close()
				r = <-done
			}
			if !errors.Is(r.err, errProtocol) {
				t.Errorf("%s = %v; want the protocol violation, neither a time-out nor ErrPeerClosed", name, r.err)
			}
			if err := c.Err(); !errors.Is(err, errProtocol) {
				t.Errorf("Err = %v; want the protocol violation", err)
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
