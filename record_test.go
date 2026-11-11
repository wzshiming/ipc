package ipc

import (
	"bytes"
	"errors"
	"runtime/debug"
	"testing"
	"time"
)

func raceEnabled() bool {
	bi, ok := debug.ReadBuildInfo()
	if !ok {
		return false
	}
	for _, s := range bi.Settings {
		if s.Key == "-race" {
			return s.Value == "true"
		}
	}
	return false
}

func TestWriteReadBothDirections(t *testing.T) {
	lc, dc := pair(t, ListenConfig{}, Dialer{})
	buf := make([]byte, 64)
	for _, ends := range [][2]*Conn{{lc, dc}, {dc, lc}} {
		from, to := ends[0], ends[1]
		msg := []byte("message")
		if n, err := from.Write(msg); n != len(msg) || err != nil {
			t.Fatalf("Write = %d, %v; want %d, nil", n, err, len(msg))
		}
		n, err := to.Read(buf)
		if err != nil || !bytes.Equal(buf[:n], msg) {
			t.Fatalf("Read = %q, %v; want %q", buf[:n], err, msg)
		}
	}
}

func TestReserveAboveMaxRecordIsErrTooLarge(t *testing.T) {
	lc, _ := pair(t, ListenConfig{RingSize: 64 << 10}, Dialer{RingSize: 64 << 10})
	if _, err := lc.Reserve(lc.MaxRecord() + 1); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("Reserve = %v; want ErrTooLarge", err)
	}
	if _, err := lc.TryReserve(lc.MaxRecord() + 1); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("TryReserve = %v; want ErrTooLarge", err)
	}
	if _, err := lc.TryReserve(lc.MaxRecord()); err != nil {
		t.Fatalf("TryReserve(MaxRecord) = %v", err)
	}
}

func TestTryNextOnEmptyRingIsErrWouldBlock(t *testing.T) {
	_, dc := pair(t, ListenConfig{}, Dialer{})
	if _, err := dc.TryNext(); !errors.Is(err, ErrWouldBlock) {
		t.Fatalf("TryNext = %v; want ErrWouldBlock", err)
	}
}

func TestTryReserveOnFullRingIsErrWouldBlock(t *testing.T) {
	lc, _ := pair(t, ListenConfig{RingSize: 64 << 10}, Dialer{RingSize: 64 << 10})
	for range 64 {
		if _, err := lc.TryReserve(4000); err != nil {
			if !errors.Is(err, ErrWouldBlock) {
				t.Fatalf("TryReserve = %v; want ErrWouldBlock", err)
			}
			return
		}
		lc.Commit()
	}
	t.Fatal("64 KiB ring never filled")
}

func TestOutOfOrderRecordCallsReturnErrorsNotPanics(t *testing.T) {
	tx, rx := pair(t, ListenConfig{}, Dialer{})
	if err := tx.Commit(); err == nil {
		t.Fatal("Commit without Reserve succeeded")
	}
	if err := rx.Release(); err == nil {
		t.Fatal("Release without Next succeeded")
	}
	tx.Abort()
	if _, err := tx.TryReserve(8); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.TryReserve(8); err == nil {
		t.Fatal("second TryReserve before Commit succeeded")
	}
	tx.Commit()
	if _, err := rx.TryNext(); err != nil {
		t.Fatal(err)
	}
	if _, err := rx.TryNext(); err == nil {
		t.Fatal("second TryNext before Release succeeded")
	}
}

func TestZeroLengthRecordIsVisibleToNext(t *testing.T) {
	lc, dc := pair(t, ListenConfig{}, Dialer{})
	if _, err := lc.Reserve(0); err != nil {
		t.Fatal(err)
	}
	if err := lc.Commit(); err != nil {
		t.Fatal(err)
	}
	rec, err := dc.Next()
	if err != nil || len(rec) != 0 {
		t.Fatalf("Next = %q, %v; want an empty record", rec, err)
	}
}

func TestBlockedNextIsWokenByDoorbell(t *testing.T) {
	lc, dc := pair(t, ListenConfig{Spin: -1}, Dialer{Spin: -1})
	ch := make(chan []byte, 1)
	go func() {
		buf := make([]byte, 8)
		n, _ := dc.Read(buf)
		ch <- buf[:n]
	}()
	time.Sleep(20 * time.Millisecond)
	lc.Write([]byte("ding"))
	select {
	case got := <-ch:
		if string(got) != "ding" {
			t.Fatalf("Read = %q; want ding", got)
		}
	case <-time.After(time.Second):
		t.Fatal("parked Next was not woken by the doorbell")
	}
}

func TestBlockedReserveIsWokenByRelease(t *testing.T) {
	tx, rx := pair(t, ListenConfig{RingSize: 64 << 10, Spin: -1}, Dialer{RingSize: 64 << 10, Spin: -1})
	for {
		if _, err := tx.TryReserve(4000); err != nil {
			break
		}
		tx.Commit()
	}
	ch := make(chan error, 1)
	go func() {
		_, err := tx.Write(make([]byte, 4000))
		ch <- err
	}()
	time.Sleep(20 * time.Millisecond)
	for range 2 {
		if _, err := rx.TryNext(); err != nil {
			t.Fatal(err)
		}
		rx.Release()
	}
	if err := waitErr(t, ch, time.Second); err != nil {
		t.Fatal(err)
	}
}

func TestSteadyStateHotPathAllocatesNothing(t *testing.T) {
	tx, rx := pair(t, ListenConfig{}, Dialer{})
	allocs := testing.AllocsPerRun(1000, func() {
		p, err := tx.TryReserve(64)
		if err != nil {
			panic(err)
		}
		p[0] = 1
		tx.Commit()
		if _, err := rx.TryNext(); err != nil {
			panic(err)
		}
		rx.Release()
	})
	if allocs != 0 {
		t.Fatalf("TryReserve/Commit/TryNext/Release allocate %v per run; want 0", allocs)
	}
}

func TestBlockingCallsWithDataOrRoomAllocateNothing(t *testing.T) {
	tx, rx := pair(t, ListenConfig{}, Dialer{})
	allocs := testing.AllocsPerRun(1000, func() {
		p, err := tx.Reserve(64)
		if err != nil {
			panic(err)
		}
		p[0] = 1
		tx.Commit()
		if _, err := rx.Next(); err != nil {
			panic(err)
		}
		rx.Release()
	})
	if allocs != 0 {
		t.Fatalf("Reserve/Commit/Next/Release allocate %v per run; want 0", allocs)
	}
	msg, buf := make([]byte, 64), make([]byte, 64)
	allocs = testing.AllocsPerRun(1000, func() {
		if _, err := tx.Write(msg); err != nil {
			panic(err)
		}
		if _, err := rx.Read(buf); err != nil {
			panic(err)
		}
	})
	if allocs != 0 {
		t.Fatalf("Write/Read allocate %v per run; want 0", allocs)
	}
}

func transfer64KiB(tb testing.TB, lc, dc *Conn, records int) time.Duration {
	msg := make([]byte, 64<<10)
	done := make(chan error, 1)
	start := time.Now()
	go func() {
		for range records {
			if _, err := lc.Write(msg); err != nil {
				done <- err
				return
			}
		}
		done <- nil
	}()
	buf := make([]byte, 64<<10)
	for range records {
		if _, err := dc.Read(buf); err != nil {
			tb.Fatal(err)
		}
	}
	if err := <-done; err != nil {
		tb.Fatal(err)
	}
	return time.Since(start)
}

func BenchmarkWriteRead64KiB(b *testing.B) {
	lc, dc := pair(b, ListenConfig{}, Dialer{})
	b.SetBytes(64 << 10)
	b.ResetTimer()
	transfer64KiB(b, lc, dc, b.N)
}
