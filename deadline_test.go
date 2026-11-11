package ipc

import (
	"errors"
	"io"
	"net"
	"os"
	"strings"
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
	awaitReceiveParked(t, dc)
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

func TestCallsAfterClosePastTheirDeadlineAreNetErrClosed(t *testing.T) {
	_, dc := pair(t, ListenConfig{}, Dialer{})
	dc.SetDeadline(time.Now().Add(-time.Second))
	dc.Close()
	buf := make([]byte, 8)
	for name, call := range map[string]func() error{
		"Read":     func() error { _, err := dc.Read(buf); return err },
		"Write":    func() error { _, err := dc.Write(buf); return err },
		"ReadFrom": func() error { _, err := dc.ReadFrom(strings.NewReader("x")); return err },
		"WriteTo":  func() error { _, err := dc.WriteTo(io.Discard); return err },
		"Next":     func() error { _, err := dc.Next(); return err },
		"Reserve":  func() error { _, err := dc.Reserve(8); return err },
	} {
		if err := call(); err != net.ErrClosed {
			t.Errorf("%s past its deadline after Close = %v; want net.ErrClosed", name, err)
		}
	}
}

func TestEmptyReadAndWriteAfterCloseAreNetErrClosed(t *testing.T) {
	_, dc := pair(t, ListenConfig{}, Dialer{})
	dc.Close()
	if n, err := dc.Read(nil); n != 0 || err != net.ErrClosed {
		t.Errorf("Read(nil) after Close = %d, %v; want 0, net.ErrClosed", n, err)
	}
	if n, err := dc.Write(nil); n != 0 || err != net.ErrClosed {
		t.Errorf("Write(nil) after Close = %d, %v; want 0, net.ErrClosed", n, err)
	}
}
