package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"time"
)

// stride is the spacing of sequence words in a throughput record: one per
// 128-byte cache line.
const stride = 128

// integrityError reports a record whose payload is not what was produced.
type integrityError struct {
	conn   int
	record uint64
	detail string
}

func (e *integrityError) Error() string {
	return fmt.Sprintf("integrity: connection %d record %d: %s", e.conn, e.record, e.detail)
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

// produceAll runs one locked producer per connection for o.duration; each
// ends its connection with Close.
func produceAll(conns []net.Conn, o options) error {
	var stop atomic.Bool
	stop.Store(o.duration <= 0)
	t := time.AfterFunc(o.duration, func() { stop.Store(true) })
	defer t.Stop()
	errs := make([]error, len(conns))
	var wg sync.WaitGroup
	for i, c := range conns {
		wg.Go(func() {
			corrupt := uint64(0)
			if i == 0 {
				corrupt = o.corrupt
			}
			errs[i] = produce(c, i, o.size, o.mode == "throughput", corrupt, &stop)
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

// produce writes size-byte records on connection index until stop is set,
// then closes it, which ends the consumer's stream. With fill, each record
// is the template stamped with its sequence number, and record corrupt gets
// a damaged sequence word.
func produce(c net.Conn, index, size int, fill bool, corrupt uint64, stop *atomic.Bool) error {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	rec := newTemplate(size)
	for seq := uint64(1); !stop.Load(); seq++ {
		if fill {
			stampTemplate(rec, seq)
			if seq == corrupt {
				rec[0] ^= 0xff
			}
		}
		if _, err := c.Write(rec); err != nil {
			return fmt.Errorf("connection %d: %w", index, err)
		}
	}
	return c.Close()
}

// connStats is what one consumer measured.
type connStats struct {
	records    uint64
	bytes      uint64
	start, end time.Time
}

// consume runs one locked consumer per connection until each connection's
// stream ends, then fills the report. The first failure closes all to stop all.
func (r *report) consume(conns []net.Conn, o options) error {
	var (
		once  sync.Once
		first error
	)
	stats := make([]connStats, len(conns))
	var wg sync.WaitGroup
	for i, c := range conns {
		wg.Go(func() {
			var err error
			stats[i], err = consume(c, i, o.size, o.mode == "throughput")
			if err != nil {
				once.Do(func() {
					first = err
					closeAll(conns)
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

// consume reads size-byte records on connection index until the producer's
// Close ends the stream with io.EOF. The window starts after the first
// record so its bytes and setup are not counted.
func consume(c net.Conn, index, size int, check bool) (connStats, error) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	var s connStats
	rec := make([]byte, size)
	for seq := uint64(1); ; seq++ {
		// Read copies the record out of the ring into rec; that copy is part of what is measured.
		n, err := io.ReadFull(c, rec)
		switch err {
		case nil:
		case io.EOF:
			s.end = time.Now()
			return s, nil
		case io.ErrUnexpectedEOF:
			return s, &integrityError{index, seq, fmt.Sprintf("length %d, want %d", n, size)}
		default:
			return s, fmt.Errorf("connection %d: %w", index, err)
		}
		if check {
			if err := verifyRecord(rec, seq); err != nil {
				return s, &integrityError{index, seq, err.Error()}
			}
		}
		if seq == 1 {
			s.start = time.Now()
			continue
		}
		s.records++
		s.bytes += uint64(n)
	}
}

// measureLatency writes size-byte records on c and times each echo until
// the run ends, discarding a warm-up.
func measureLatency(c net.Conn, size int, d time.Duration) (*latency, error) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	const warmup = 1000
	msg, reply := newTemplate(size), make([]byte, size)
	samples := make([]time.Duration, 0, 1<<20)
	start := nowNanos()
	for seq := uint64(1); ; seq++ {
		t0 := nowNanos()
		if seq > warmup && elapsed(start, t0) >= d {
			break
		}
		binary.LittleEndian.PutUint64(msg, seq)
		if _, err := c.Write(msg); err != nil {
			return nil, err
		}
		if _, err := io.ReadFull(c, reply); err != nil {
			return nil, err
		}
		if binary.LittleEndian.Uint64(reply) != seq {
			return nil, &integrityError{0, seq, "echo differs from the record sent"}
		}
		if seq > warmup {
			samples = append(samples, elapsed(t0, nowNanos()))
		}
	}
	return newLatency(samples), nil
}

// echo writes every size-byte record read on c back until the parent's
// Close ends the stream with io.EOF.
func echo(c net.Conn, size int) error {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	rec := make([]byte, size)
	for {
		if _, err := io.ReadFull(c, rec); err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		if _, err := c.Write(rec); err != nil {
			return err
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
