package main

import (
	"encoding/binary"
	"fmt"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/wzshiming/ipc"
)

// stride is the spacing of sequence words in a throughput record: one per
// 128-byte cache line.
const stride = 128

// integrityError reports a record whose payload is not what was produced.
type integrityError struct {
	lane   int
	record uint64
	detail string
}

func (e *integrityError) Error() string {
	return fmt.Sprintf("integrity: lane %d record %d: %s", e.lane, e.record, e.detail)
}

// newTemplate returns a size-byte record template filled with a pattern.
func newTemplate(size int) []byte {
	t := make([]byte, size)
	for i := range t {
		t[i] = byte(i*7 + 1)
	}
	return t
}

// stampTemplate writes seq into every sequence word of t.
func stampTemplate(t []byte, seq uint64) {
	for off := 0; off+8 <= len(t); off += stride {
		binary.LittleEndian.PutUint64(t[off:], seq)
	}
}

// verifyRecord checks that every sequence word of rec holds seq.
func verifyRecord(rec []byte, seq uint64) error {
	for off := 0; off+8 <= len(rec); off += stride {
		if w := binary.LittleEndian.Uint64(rec[off:]); w != seq {
			return fmt.Errorf("offset %d holds %#x, want %#x", off, w, seq)
		}
	}
	return nil
}

// produceAll runs one locked producer per lane for o.duration, then ends
// each lane with a zero-length record.
func produceAll(conn *ipc.Conn, o options) error {
	var stop atomic.Bool
	stop.Store(o.duration <= 0)
	t := time.AfterFunc(o.duration, func() { stop.Store(true) })
	defer t.Stop()
	errs := make([]error, conn.Lanes())
	var wg sync.WaitGroup
	for i := range conn.Lanes() {
		wg.Go(func() {
			corrupt := uint64(0)
			if i == 0 {
				corrupt = o.corrupt
			}
			errs[i] = produce(conn.Lane(i), o.size, o.mode == "throughput", corrupt, &stop)
		})
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}

// produce sends size-byte records until stop is set. With fill, each record
// is a copy of the template stamped with its sequence number, and record
// corrupt gets a damaged sequence word.
func produce(l *ipc.Lane, size int, fill bool, corrupt uint64, stop *atomic.Bool) error {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	tmpl := newTemplate(size)
	for seq := uint64(1); !stop.Load(); seq++ {
		b, err := l.Reserve(size)
		if err != nil {
			return fmt.Errorf("lane %d: %w", l.Index(), err)
		}
		if fill {
			stampTemplate(tmpl, seq)
			if seq == corrupt {
				tmpl[0] ^= 0xff
			}
			copy(b, tmpl)
		}
		if err := l.Commit(); err != nil {
			return fmt.Errorf("lane %d: %w", l.Index(), err)
		}
	}
	if _, err := l.Reserve(0); err != nil {
		return fmt.Errorf("lane %d: %w", l.Index(), err)
	}
	return l.Commit()
}

// laneStats is what one consumer measured.
type laneStats struct {
	records    uint64
	bytes      uint64
	start, end time.Time
}

// consume runs one locked consumer per lane until each lane's end
// marker, then fills the report. The first failure closes conn to stop all.
func (r *report) consume(conn *ipc.Conn, o options) error {
	var (
		once  sync.Once
		first error
	)
	stats := make([]laneStats, conn.Lanes())
	var wg sync.WaitGroup
	for i := range conn.Lanes() {
		wg.Go(func() {
			var err error
			stats[i], err = consume(conn.Lane(i), o.size, o.mode == "throughput")
			if err != nil {
				once.Do(func() {
					first = err
					conn.Close()
				})
			}
		})
	}
	wg.Wait()
	if first != nil {
		return first
	}
	r.addThroughput(stats)
	return nil
}

// consume receives records until the zero-length end marker. The window
// starts after the first record so its bytes and setup are not counted.
func consume(l *ipc.Lane, size int, check bool) (laneStats, error) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	var s laneStats
	for seq := uint64(1); ; seq++ {
		rec, err := l.Next()
		if err != nil {
			return s, fmt.Errorf("lane %d: %w", l.Index(), err)
		}
		n := len(rec)
		if n == 0 {
			s.end = time.Now()
			return s, l.Release()
		}
		if n != size {
			l.Release()
			return s, &integrityError{l.Index(), seq, fmt.Sprintf("length %d, want %d", n, size)}
		}
		if check {
			if err := verifyRecord(rec, seq); err != nil {
				l.Release()
				return s, &integrityError{l.Index(), seq, err.Error()}
			}
		}
		if err := l.Release(); err != nil {
			return s, fmt.Errorf("lane %d: %w", l.Index(), err)
		}
		if seq == 1 {
			s.start = time.Now()
			continue
		}
		s.records++
		s.bytes += uint64(n)
	}
}

// measureLatency sends size-byte records on l and times each echo until
// the run ends, discarding a warm-up.
func measureLatency(l *ipc.Lane, size int, d time.Duration) (*latency, error) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	const warmup = 1000
	msg := newTemplate(size)
	samples := make([]time.Duration, 0, 1<<20)
	start := nowNanos()
	for seq := uint64(1); ; seq++ {
		t0 := nowNanos()
		if seq > warmup && elapsed(start, t0) >= d {
			break
		}
		binary.LittleEndian.PutUint64(msg, seq)
		if _, err := l.Write(msg); err != nil {
			return nil, err
		}
		rec, err := l.Next()
		if err != nil {
			return nil, err
		}
		if len(rec) != size || binary.LittleEndian.Uint64(rec) != seq {
			l.Release()
			return nil, &integrityError{0, seq, "echo differs from the record sent"}
		}
		if err := l.Release(); err != nil {
			return nil, err
		}
		if seq > warmup {
			samples = append(samples, elapsed(t0, nowNanos()))
		}
	}
	if _, err := l.Write(nil); err != nil {
		return nil, err
	}
	if _, err := l.Next(); err != nil {
		return nil, err
	}
	l.Release()
	return newLatency(samples), nil
}

// echo returns every record on l until it echoes the zero-length end.
func echo(l *ipc.Lane) error {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	for {
		rec, err := l.Next()
		if err != nil {
			return err
		}
		b, err := l.Reserve(len(rec))
		if err != nil {
			return err
		}
		copy(b, rec)
		n := len(rec)
		if err := l.Release(); err != nil {
			return err
		}
		if err := l.Commit(); err != nil {
			return err
		}
		if n == 0 {
			return nil
		}
	}
}

// newLatency summarizes round-trip samples in microseconds.
func newLatency(samples []time.Duration) *latency {
	lat := &latency{Samples: len(samples)}
	if len(samples) == 0 {
		return lat
	}
	slices.Sort(samples)
	us := func(d time.Duration) float64 { return float64(d) / float64(time.Microsecond) }
	pct := func(p float64) float64 {
		i := int(p*float64(len(samples))+0.999999) - 1
		return us(samples[max(0, min(i, len(samples)-1))])
	}
	var sum time.Duration
	for _, s := range samples {
		sum += s
	}
	lat.Min, lat.Max = us(samples[0]), us(samples[len(samples)-1])
	lat.Mean = us(sum / time.Duration(len(samples)))
	lat.P50, lat.P99, lat.P999 = pct(0.50), pct(0.99), pct(0.999)
	return lat
}
