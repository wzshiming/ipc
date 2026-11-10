package ipc

import (
	"bytes"
	"errors"
	"io"
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

func TestWriteReadBothDirectionsOnEveryLane(t *testing.T) {
	lc, dc := pair(t, ListenConfig{Lanes: 3}, Dialer{Lanes: 3})
	buf := make([]byte, 64)
	for i := range 3 {
		for _, ends := range [][2]*Conn{{lc, dc}, {dc, lc}} {
			from, to := ends[0].Lane(i), ends[1].Lane(i)
			if from.Index() != i || from.Conn() != ends[0] {
				t.Fatalf("Lane(%d).Index, Conn = %d, %p", i, from.Index(), from.Conn())
			}
			msg := []byte{byte(i), 'm'}
			if n, err := from.Write(msg); n != len(msg) || err != nil {
				t.Fatalf("Write = %d, %v; want %d, nil", n, err, len(msg))
			}
			n, err := to.Read(buf)
			if err != nil || !bytes.Equal(buf[:n], msg) {
				t.Fatalf("lane %d Read = %q, %v; want %q", i, buf[:n], err, msg)
			}
		}
	}
}

func TestReadShortBufferConsumesRecord(t *testing.T) {
	lc, dc := pair(t, ListenConfig{}, Dialer{})
	lc.Lane(0).Write([]byte("abcdef"))
	lc.Lane(0).Write([]byte("next"))
	buf := make([]byte, 3)
	n, err := dc.Lane(0).Read(buf)
	if !errors.Is(err, io.ErrShortBuffer) || n != 3 || string(buf[:n]) != "abc" {
		t.Fatalf("Read = %d %q, %v; want 3 abc, io.ErrShortBuffer", n, buf[:n], err)
	}
	buf = make([]byte, 8)
	n, err = dc.Lane(0).Read(buf)
	if err != nil || string(buf[:n]) != "next" {
		t.Fatalf("Read after short = %q, %v; want next", buf[:n], err)
	}
}

func TestWriteEmptySendsZeroLengthRecord(t *testing.T) {
	lc, dc := pair(t, ListenConfig{}, Dialer{})
	if n, err := lc.Lane(0).Write(nil); n != 0 || err != nil {
		t.Fatalf("Write(nil) = %d, %v; want 0, nil", n, err)
	}
	rec, err := dc.Lane(0).TryNext()
	if err != nil || len(rec) != 0 {
		t.Fatalf("TryNext = %q, %v; want an empty record", rec, err)
	}
}

func TestWriteAboveMaxRecordIsErrTooLarge(t *testing.T) {
	lc, _ := pair(t, ListenConfig{RingSize: 64 << 10}, Dialer{RingSize: 64 << 10})
	n, err := lc.Lane(0).Write(make([]byte, lc.MaxRecord()+1))
	if n != 0 || !errors.Is(err, ErrTooLarge) {
		t.Fatalf("Write = %d, %v; want 0, ErrTooLarge", n, err)
	}
	if _, err := lc.Lane(0).Reserve(lc.MaxRecord() + 1); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("Reserve = %v; want ErrTooLarge", err)
	}
	if _, err := lc.Lane(0).Write(make([]byte, lc.MaxRecord())); err != nil {
		t.Fatalf("Write(MaxRecord) = %v", err)
	}
}

func TestTryNextOnEmptyLaneIsErrWouldBlock(t *testing.T) {
	_, dc := pair(t, ListenConfig{}, Dialer{})
	if _, err := dc.Lane(0).TryNext(); !errors.Is(err, ErrWouldBlock) {
		t.Fatalf("TryNext = %v; want ErrWouldBlock", err)
	}
}

func TestTryReserveOnFullLaneIsErrWouldBlock(t *testing.T) {
	lc, _ := pair(t, ListenConfig{RingSize: 64 << 10}, Dialer{RingSize: 64 << 10})
	tx := lc.Lane(0)
	for range 64 {
		if _, err := tx.TryReserve(4000); err != nil {
			if !errors.Is(err, ErrWouldBlock) {
				t.Fatalf("TryReserve = %v; want ErrWouldBlock", err)
			}
			return
		}
		tx.Commit()
	}
	t.Fatal("64 KiB ring never filled")
}

func TestOutOfOrderLaneCallsReturnErrorsNotPanics(t *testing.T) {
	lc, dc := pair(t, ListenConfig{}, Dialer{})
	tx, rx := lc.Lane(0), dc.Lane(0)
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

func TestBlockedNextIsWokenByDoorbell(t *testing.T) {
	lc, dc := pair(t, ListenConfig{Spin: -1}, Dialer{Spin: -1})
	ch := make(chan []byte, 1)
	go func() {
		buf := make([]byte, 8)
		n, _ := dc.Lane(0).Read(buf)
		ch <- buf[:n]
	}()
	time.Sleep(20 * time.Millisecond)
	lc.Lane(0).Write([]byte("ding"))
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
	lc, dc := pair(t, ListenConfig{RingSize: 64 << 10, Spin: -1}, Dialer{RingSize: 64 << 10, Spin: -1})
	tx, rx := lc.Lane(0), dc.Lane(0)
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
	lc, dc := pair(t, ListenConfig{}, Dialer{})
	tx, rx := lc.Lane(0), dc.Lane(0)
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
	lc, dc := pair(t, ListenConfig{}, Dialer{})
	tx, rx := lc.Lane(0), dc.Lane(0)
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
			if _, err := lc.Lane(0).Write(msg); err != nil {
				done <- err
				return
			}
		}
		done <- nil
	}()
	buf := make([]byte, 64<<10)
	for range records {
		if _, err := dc.Lane(0).Read(buf); err != nil {
			tb.Fatal(err)
		}
	}
	if err := <-done; err != nil {
		tb.Fatal(err)
	}
	return time.Since(start)
}

func TestSingleLane64KiBThroughput(t *testing.T) {
	lc, dc := pair(t, ListenConfig{}, Dialer{})
	records := 16384
	if raceEnabled() {
		records = 1024
	}
	d := transfer64KiB(t, lc, dc, records)
	rate := float64(records) * (64 << 10) / d.Seconds()
	t.Logf("single lane, 64 KiB records: %.2f GB/s (race=%v)", rate/1e9, raceEnabled())
	if !raceEnabled() && rate < 1e9 {
		t.Fatalf("throughput %.2f GB/s below 1 GB/s", rate/1e9)
	}
}

func BenchmarkLane64KiB(b *testing.B) {
	lc, dc := pair(b, ListenConfig{}, Dialer{})
	b.SetBytes(64 << 10)
	b.ResetTimer()
	transfer64KiB(b, lc, dc, b.N)
}
