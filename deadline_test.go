package ipc

import (
	"errors"
	"net"
	"os"
	"testing"
	"time"
)

// fill commits records on l until the ring is full.
func fill(t *testing.T, l *Lane) {
	t.Helper()
	for {
		_, err := l.TryReserve(4000)
		if errors.Is(err, ErrWouldBlock) {
			return
		}
		if err != nil {
			t.Fatal(err)
		}
		if err := l.Commit(); err != nil {
			t.Fatal(err)
		}
	}
}

func isTimeout(err error) bool {
	var ne net.Error
	return errors.Is(err, os.ErrDeadlineExceeded) && errors.As(err, &ne) && ne.Timeout()
}

func TestPastReadDeadlineFailsNextAndReadAtOnce(t *testing.T) {
	_, dc := pair(t, Config{}, Config{})
	l := dc.Lane(0)
	if err := l.SetReadDeadline(time.Now().Add(-time.Second)); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	if _, err := l.Next(); !isTimeout(err) {
		t.Fatalf("Next past deadline = %v; want os.ErrDeadlineExceeded as a net.Error timeout", err)
	}
	if _, err := l.Read(make([]byte, 8)); !isTimeout(err) {
		t.Fatalf("Read past deadline = %v; want os.ErrDeadlineExceeded as a net.Error timeout", err)
	}
	if d := time.Since(start); d > 50*time.Millisecond {
		t.Fatalf("calls past deadline took %v; want immediate", d)
	}
}

func TestSetReadDeadlineInterruptsBlockedNext(t *testing.T) {
	_, dc := pair(t, Config{}, Config{Spin: -1})
	l := dc.Lane(0)
	ch := make(chan error, 1)
	go func() {
		_, err := l.Next()
		ch <- err
	}()
	time.Sleep(20 * time.Millisecond)
	dl := time.Now().Add(30 * time.Millisecond)
	l.SetReadDeadline(dl)
	err := waitErr(t, ch, time.Second)
	if !isTimeout(err) {
		t.Fatalf("blocked Next = %v; want os.ErrDeadlineExceeded", err)
	}
	if late := time.Since(dl); late > 100*time.Millisecond {
		t.Fatalf("blocked Next returned %v after its deadline", late)
	}
}

func TestZeroReadDeadlineClearsAndNextBlocksUntilData(t *testing.T) {
	lc, dc := pair(t, Config{}, Config{Spin: -1})
	l := dc.Lane(0)
	l.SetReadDeadline(time.Now().Add(-time.Second))
	l.SetReadDeadline(time.Time{})
	ch := make(chan error, 1)
	go func() {
		_, err := l.Read(make([]byte, 8))
		ch <- err
	}()
	select {
	case err := <-ch:
		t.Fatalf("Next after clearing the deadline returned %v without data", err)
	case <-time.After(50 * time.Millisecond):
	}
	if _, err := lc.Lane(0).Write([]byte("x")); err != nil {
		t.Fatal(err)
	}
	if err := waitErr(t, ch, time.Second); err != nil {
		t.Fatalf("Read after data = %v", err)
	}
}

func TestWriteDeadlineTimesOutReserveAndWriteOnFullRing(t *testing.T) {
	lc, _ := pair(t, Config{RingSize: 64 << 10}, Config{RingSize: 64 << 10})
	l := lc.Lane(0)
	fill(t, l)
	l.SetWriteDeadline(time.Now().Add(20 * time.Millisecond))
	if _, err := l.Reserve(4000); !isTimeout(err) {
		t.Fatalf("Reserve on full ring past deadline = %v; want os.ErrDeadlineExceeded", err)
	}
	l.SetWriteDeadline(time.Now().Add(20 * time.Millisecond))
	if n, err := l.Write(make([]byte, 4000)); n != 0 || !isTimeout(err) {
		t.Fatalf("Write on full ring past deadline = %d, %v; want 0, os.ErrDeadlineExceeded", n, err)
	}
}

func TestReadDeadlineTimeoutKeepsLaneUsable(t *testing.T) {
	lc, dc := pair(t, Config{}, Config{})
	l := dc.Lane(0)
	l.SetDeadline(time.Now().Add(10 * time.Millisecond))
	if _, err := l.Next(); !isTimeout(err) {
		t.Fatalf("Next = %v; want os.ErrDeadlineExceeded", err)
	}
	l.SetDeadline(time.Time{})
	if _, err := lc.Lane(0).Write([]byte("after")); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 8)
	if n, err := l.Read(buf); err != nil || string(buf[:n]) != "after" {
		t.Fatalf("Read = %q, %v; want after", buf[:n], err)
	}
}

func TestTryCallsIgnoreDeadlines(t *testing.T) {
	lc, dc := pair(t, Config{RingSize: 64 << 10}, Config{RingSize: 64 << 10})
	tx, rx := lc.Lane(0), dc.Lane(0)
	past := time.Now().Add(-time.Second)
	tx.SetDeadline(past)
	rx.SetDeadline(past)
	if _, err := rx.TryNext(); err != ErrWouldBlock {
		t.Fatalf("TryNext on empty lane past deadline = %v; want ErrWouldBlock", err)
	}
	fill(t, tx)
	if _, err := tx.TryReserve(4000); err != ErrWouldBlock {
		t.Fatalf("TryReserve on full lane past deadline = %v; want ErrWouldBlock", err)
	}
	if _, err := rx.TryNext(); err != nil {
		t.Fatalf("TryNext with a record past deadline = %v; want the record", err)
	}
}

func TestStreamDeadlinesGovernTheLane(t *testing.T) {
	_, dc := pair(t, Config{}, Config{})
	s := NewStream(dc.Lane(0))
	s.SetReadDeadline(time.Now().Add(-time.Second))
	if _, err := dc.Lane(0).Next(); !isTimeout(err) {
		t.Fatalf("Lane.Next after Stream.SetReadDeadline in the past = %v; want os.ErrDeadlineExceeded", err)
	}
}
