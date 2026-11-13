package ipc

import (
	"errors"
	"net"
	"os"
	"runtime"
	"sync"
	"testing"
	"time"
)

// fill commits records on c until the ring is full.
func fill(t *testing.T, c *Conn) {
	t.Helper()
	for {
		_, err := c.TryReserve(4000)
		if errors.Is(err, ErrWouldBlock) {
			return
		}
		if err != nil {
			t.Fatal(err)
		}
		if err := c.Commit(); err != nil {
			t.Fatal(err)
		}
	}
}

func isTimeout(err error) bool {
	var ne net.Error
	return errors.Is(err, os.ErrDeadlineExceeded) && errors.As(err, &ne) && ne.Timeout()
}

func TestPastReadDeadlineFailsNextAndReadAtOnce(t *testing.T) {
	_, dc := pair(t, ListenConfig{}, Dialer{})
	if err := dc.SetReadDeadline(time.Now().Add(-time.Second)); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	if _, err := dc.Next(); !isTimeout(err) {
		t.Fatalf("Next past deadline = %v; want os.ErrDeadlineExceeded as a net.Error timeout", err)
	}
	if _, err := dc.Read(make([]byte, 8)); !isTimeout(err) {
		t.Fatalf("Read past deadline = %v; want os.ErrDeadlineExceeded as a net.Error timeout", err)
	}
	if d := time.Since(start); d > 50*time.Millisecond {
		t.Fatalf("calls past deadline took %v; want immediate", d)
	}
}

func TestSetReadDeadlineInterruptsBlockedNext(t *testing.T) {
	_, dc := pair(t, ListenConfig{}, Dialer{Spin: -1})
	ch := make(chan error, 1)
	go func() {
		_, err := dc.Next()
		ch <- err
	}()
	time.Sleep(20 * time.Millisecond)
	dl := time.Now().Add(30 * time.Millisecond)
	dc.SetReadDeadline(dl)
	err := waitErr(t, ch, time.Second)
	if !isTimeout(err) {
		t.Fatalf("blocked Next = %v; want os.ErrDeadlineExceeded", err)
	}
	if late := time.Since(dl); late > 100*time.Millisecond {
		t.Fatalf("blocked Next returned %v after its deadline", late)
	}
}

func TestZeroReadDeadlineClearsAndReadBlocksUntilData(t *testing.T) {
	lc, dc := pair(t, ListenConfig{}, Dialer{Spin: -1})
	dc.SetReadDeadline(time.Now().Add(-time.Second))
	dc.SetReadDeadline(time.Time{})
	ch := make(chan error, 1)
	go func() {
		_, err := dc.Read(make([]byte, 8))
		ch <- err
	}()
	select {
	case err := <-ch:
		t.Fatalf("Read after clearing the deadline returned %v without data", err)
	case <-time.After(50 * time.Millisecond):
	}
	if _, err := lc.Write([]byte("x")); err != nil {
		t.Fatal(err)
	}
	if err := waitErr(t, ch, time.Second); err != nil {
		t.Fatalf("Read after data = %v", err)
	}
}

func TestWriteDeadlineTimesOutReserveAndWriteOnFullRing(t *testing.T) {
	lc, _ := pair(t, ListenConfig{RingSize: 64 << 10}, Dialer{RingSize: 64 << 10})
	fill(t, lc)
	lc.SetWriteDeadline(time.Now().Add(20 * time.Millisecond))
	if _, err := lc.Reserve(4000); !isTimeout(err) {
		t.Fatalf("Reserve on full ring past deadline = %v; want os.ErrDeadlineExceeded", err)
	}
	lc.SetWriteDeadline(time.Now().Add(20 * time.Millisecond))
	if n, err := lc.Write(make([]byte, 4000)); n != 0 || !isTimeout(err) {
		t.Fatalf("Write on full ring past deadline = %d, %v; want 0, os.ErrDeadlineExceeded", n, err)
	}
}

func TestReadDeadlineTimeoutKeepsConnUsable(t *testing.T) {
	lc, dc := pair(t, ListenConfig{}, Dialer{})
	dc.SetDeadline(time.Now().Add(10 * time.Millisecond))
	if _, err := dc.Next(); !isTimeout(err) {
		t.Fatalf("Next = %v; want os.ErrDeadlineExceeded", err)
	}
	dc.SetDeadline(time.Time{})
	if _, err := lc.Write([]byte("after")); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 8)
	if n, err := dc.Read(buf); err != nil || string(buf[:n]) != "after" {
		t.Fatalf("Read = %q, %v; want after", buf[:n], err)
	}
}

func TestTryCallsIgnoreDeadlines(t *testing.T) {
	tx, rx := pair(t, ListenConfig{RingSize: 64 << 10}, Dialer{RingSize: 64 << 10})
	past := time.Now().Add(-time.Second)
	tx.SetDeadline(past)
	rx.SetDeadline(past)
	if _, err := rx.TryNext(); err != ErrWouldBlock {
		t.Fatalf("TryNext on empty ring past deadline = %v; want ErrWouldBlock", err)
	}
	fill(t, tx)
	if _, err := tx.TryReserve(4000); err != ErrWouldBlock {
		t.Fatalf("TryReserve on full ring past deadline = %v; want ErrWouldBlock", err)
	}
	if _, err := rx.TryNext(); err != nil {
		t.Fatalf("TryNext with a record past deadline = %v; want the record", err)
	}
}

// blockingCall is a call on a fresh connection that blocks until the peer
// acts, with the deadline it honours.
type blockingCall struct {
	start       func() <-chan error // runs the call in the background
	setDeadline func(time.Time) error
	peerActs    func()
}

// blockingCalls are a Read waiting for data and a Write waiting for room.
var blockingCalls = []struct {
	name string
	open func(t *testing.T) blockingCall
}{
	{"receive", func(t *testing.T) blockingCall {
		lc, dc := pair(t, ListenConfig{}, Dialer{Spin: -1})
		return blockingCall{
			start: func() <-chan error {
				ch := make(chan error, 1)
				go func() {
					_, err := dc.Read(make([]byte, 8))
					ch <- err
				}()
				return ch
			},
			setDeadline: dc.SetReadDeadline,
			peerActs: func() {
				if _, err := lc.Write([]byte("x")); err != nil {
					t.Error("peer Write:", err)
				}
			},
		}
	}},
	{"send", func(t *testing.T) blockingCall {
		lc, dc := pair(t, ListenConfig{RingSize: 64 << 10, Spin: -1}, Dialer{RingSize: 64 << 10})
		return blockingCall{
			start: func() <-chan error {
				fill(t, lc)
				ch := make(chan error, 1)
				go func() {
					_, err := lc.Write(make([]byte, 4000))
					ch <- err
				}()
				return ch
			},
			setDeadline: lc.SetWriteDeadline,
			peerActs: func() {
				for range 2 {
					if _, err := dc.TryNext(); err != nil {
						t.Error("peer TryNext:", err)
						return
					}
					dc.Release()
				}
			},
		}
	}},
}

func TestBlockedCallKeepsWaitingWhenDeadlineIsExtendedOrCleared(t *testing.T) {
	for _, call := range blockingCalls {
		t.Run(call.name, func(t *testing.T) {
			for _, change := range []struct {
				name  string
				after time.Duration // from the change; 0 clears the deadline
			}{{"extended", time.Minute}, {"cleared", 0}} {
				t.Run(change.name, func(t *testing.T) {
					c := call.open(t)
					dl := time.Now().Add(100 * time.Millisecond)
					c.setDeadline(dl)
					ch := c.start()
					time.Sleep(20 * time.Millisecond)
					var later time.Time
					if change.after > 0 {
						later = time.Now().Add(change.after)
					}
					c.setDeadline(later)
					time.Sleep(time.Until(dl) + 50*time.Millisecond)
					select {
					case err := <-ch:
						t.Fatalf("blocked call returned %v past the deadline it had before it was %s", err, change.name)
					default:
					}
					c.peerActs()
					if err := waitErr(t, ch, time.Second); err != nil {
						t.Fatalf("blocked call = %v once the peer acted; want success", err)
					}
				})
			}
		})
	}
}

func TestBlockedCallTimesOutWhenDeadlineIsSetOrShortened(t *testing.T) {
	for _, call := range blockingCalls {
		t.Run(call.name, func(t *testing.T) {
			for _, change := range []struct {
				name   string
				before time.Duration // the deadline the call starts under; 0 for none
			}{{"set", 0}, {"shortened", time.Minute}} {
				t.Run(change.name, func(t *testing.T) {
					c := call.open(t)
					if change.before > 0 {
						c.setDeadline(time.Now().Add(change.before))
					}
					ch := c.start()
					time.Sleep(20 * time.Millisecond)
					dl := time.Now().Add(50 * time.Millisecond)
					c.setDeadline(dl)
					if err := waitErr(t, ch, time.Second); !isTimeout(err) {
						t.Fatalf("blocked call = %v once its deadline was %s; want os.ErrDeadlineExceeded", err, change.name)
					}
					if early := time.Until(dl); early > 0 {
						t.Fatalf("blocked call timed out %v before the deadline it was %s to", early, change.name)
					}
				})
			}
		})
	}
}

func TestFutureDeadlineAfterExpiryLetsNextCallWaitAndSucceed(t *testing.T) {
	for _, call := range blockingCalls {
		t.Run(call.name, func(t *testing.T) {
			c := call.open(t)
			c.setDeadline(time.Now().Add(10 * time.Millisecond))
			if err := waitErr(t, c.start(), time.Second); !isTimeout(err) {
				t.Fatalf("call past its deadline = %v; want os.ErrDeadlineExceeded", err)
			}
			c.setDeadline(time.Now().Add(time.Minute))
			ch := c.start()
			time.Sleep(20 * time.Millisecond)
			select {
			case err := <-ch:
				t.Fatalf("call under a future deadline returned %v before the peer acted", err)
			default:
			}
			c.peerActs()
			if err := waitErr(t, ch, time.Second); err != nil {
				t.Fatalf("call under a future deadline = %v once the peer acted; want success", err)
			}
		})
	}
}

func TestDeadlineReplacedConcurrentlyLosesNoWakeup(t *testing.T) {
	for _, call := range blockingCalls {
		t.Run(call.name, func(t *testing.T) {
			c := call.open(t)
			stop := churnDeadline(c.setDeadline)
			defer stop()
			ch := c.start()
			time.Sleep(50 * time.Millisecond)
			c.peerActs()
			if err := waitErr(t, ch, time.Second); err != nil {
				t.Fatalf("call woken by the peer amid deadline changes = %v; want success", err)
			}
			ch = c.start()
			time.Sleep(20 * time.Millisecond)
			stop()
			c.setDeadline(time.Now().Add(20 * time.Millisecond))
			if err := waitErr(t, ch, time.Second); !isTimeout(err) {
				t.Fatalf("call given a near deadline after deadline changes = %v; want os.ErrDeadlineExceeded", err)
			}
		})
	}
}

// churnDeadline replaces a deadline from four goroutines, alternating none
// and one an hour away, until stop is called.
func churnDeadline(set func(time.Time) error) (stop func()) {
	done := make(chan struct{})
	var wg sync.WaitGroup
	for i := range 4 {
		wg.Go(func() {
			for j := i; ; j++ {
				select {
				case <-done:
					return
				default:
				}
				if j%2 == 0 {
					set(time.Time{})
				} else {
					set(time.Now().Add(time.Hour))
				}
			}
		})
	}
	return sync.OnceFunc(func() {
		close(done)
		wg.Wait()
	})
}

// deadlinePingPong runs b.N 64 B round trips like pinnedPingPong. With
// eachCall every Read and Write first sets its deadline a minute ahead;
// without, both ends set a deadline an hour ahead once.
func deadlinePingPong(b *testing.B, eachCall bool) {
	lc, dc := pair(b, ListenConfig{}, Dialer{})
	if !eachCall {
		far := time.Now().Add(time.Hour)
		lc.SetDeadline(far)
		dc.SetDeadline(far)
	}
	read := func(c *Conn, p []byte) (int, error) {
		if eachCall {
			c.SetReadDeadline(time.Now().Add(time.Minute))
		}
		return c.Read(p)
	}
	write := func(c *Conn, p []byte) (int, error) {
		if eachCall {
			c.SetWriteDeadline(time.Now().Add(time.Minute))
		}
		return c.Write(p)
	}
	done := make(chan error, 1)
	go func() {
		runtime.LockOSThread()
		defer runtime.UnlockOSThread()
		buf := make([]byte, 64)
		for range b.N {
			n, err := read(dc, buf)
			if err == nil {
				_, err = write(dc, buf[:n])
			}
			if err != nil {
				done <- err
				return
			}
		}
		done <- nil
	}()
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	msg, buf := make([]byte, 64), make([]byte, 64)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := write(lc, msg); err != nil {
			b.Fatal(err)
		}
		if _, err := read(lc, buf); err != nil {
			b.Fatal(err)
		}
	}
	if err := <-done; err != nil {
		b.Fatal(err)
	}
}

func BenchmarkPinnedPingPong64BDeadlineEachCall(b *testing.B) { deadlinePingPong(b, true) }

func BenchmarkPinnedPingPong64BFarDeadline(b *testing.B) { deadlinePingPong(b, false) }
