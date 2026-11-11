package main

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"time"
)

// stride is the spacing of stamp words in the throughput stream: the first
// 8 bytes of every 128-byte line hold the line's stream offset.
const stride = 128

// integrityError reports a payload that is not what was produced.
type integrityError struct {
	conn   int
	detail string
}

func (e *integrityError) Error() string {
	return fmt.Sprintf("integrity: connection %d: %s", e.conn, e.detail)
}

// newTemplate returns a size-byte record template filled with a pattern.
func newTemplate(size int) []byte {
	t := make([]byte, size)
	for i := range t {
		t[i] = byte(i*7 + 1)
	}
	return t
}

// filler is the pattern between stamp words; its length is a multiple of the
// pattern's period, so filler[o%len(filler)] is the byte at stream offset o.
var filler = newTemplate(64 << 10)

// fill writes the stream's bytes from offset off into p: the filler, then
// each line's stamp word, byte by byte where one crosses an end of p.
func fill(p []byte, off uint64) {
	for n := 0; n < len(p); {
		n += copy(p[n:], filler[(off+uint64(n))%uint64(len(filler)):])
	}
	end := off + uint64(len(p))
	for line := off - off%stride; line < end; line += stride {
		if line >= off && line+8 <= end {
			binary.LittleEndian.PutUint64(p[line-off:], line)
			continue
		}
		for o := max(line, off); o < min(line+8, end); o++ {
			p[o-off] = stampByte(line, o)
		}
	}
}

// stampByte is the byte at stream offset o of the stamp word at line.
func stampByte(line, o uint64) byte { return byte(line >> (8 * (o - line))) }

// verify checks the stamp words of p, the stream's bytes from offset off,
// and names the first offset that differs; filler bytes are not checked.
func verify(p []byte, off uint64) error {
	end := off + uint64(len(p))
	for line := off - off%stride; line < end; line += stride {
		if line >= off && line+8 <= end {
			if w := binary.LittleEndian.Uint64(p[line-off:]); w != line {
				return fmt.Errorf("offset %d holds %#x, want %#x", line, w, line)
			}
			continue
		}
		for o := max(line, off); o < min(line+8, end); o++ {
			if b, want := p[o-off], stampByte(line, o); b != want {
				return fmt.Errorf("offset %d holds %#x, want %#x", o, b, want)
			}
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
			errs[i] = produce(c, i, o, &stop)
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

// source is the producer's io.Reader: each Read takes at most size bytes of
// the slice, filled with the stream's bytes when payload is set, and
// reports io.EOF once stop is set. Read number corrupt gets its first byte
// flipped.
type source struct {
	size    int
	payload bool
	corrupt uint64
	stop    *atomic.Bool
	off     uint64 // stream offset of the next byte
	reads   uint64
}

func (s *source) Read(p []byte) (int, error) {
	if s.stop.Load() {
		return 0, io.EOF
	}
	n := min(len(p), s.size)
	s.reads++
	if s.payload && n > 0 {
		fill(p[:n], s.off)
		if s.reads == s.corrupt {
			p[0] ^= 0xff
		}
	}
	s.off += uint64(n)
	return n, nil
}

// produce sends a source of o.size-byte chunks on connection index until
// stop is set, then closes it, which ends the consumer's stream. With -io
// copy, io.Copy has the source fill each ring record in place; with rw each
// chunk is written from one buffer. Throughput chunks hold the stream's
// pattern, and chunk o.corrupt of connection 0 gets a damaged first byte.
func produce(c net.Conn, index int, o options, stop *atomic.Bool) error {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	src := &source{size: o.size, payload: o.mode == "throughput", stop: stop}
	if index == 0 {
		src.corrupt = o.corrupt
	}
	var err error
	if o.io == "rw" {
		err = writeMessages(c, src)
	} else {
		// ReadFrom has src.Read fill each ring record in place; nothing is copied.
		_, err = io.Copy(c, src)
	}
	if err != nil {
		return fmt.Errorf("connection %d: %w", index, err)
	}
	return c.Close()
}

// writeMessages writes src's chunks on c from one buffer until io.EOF.
func writeMessages(c net.Conn, src *source) error {
	buf := make([]byte, src.size)
	for {
		n, err := src.Read(buf)
		if err == io.EOF {
			return nil
		}
		if _, err = c.Write(buf[:n]); err != nil {
			return err
		}
	}
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
			stats[i], err = consume(c, i, o)
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

// sink is the consumer's io.Writer: each Write is one delivered record. The
// first starts the window and is not counted; with check, every one is
// verified as the stream's bytes at the offset it continues.
type sink struct {
	conn  int
	check bool
	off   uint64 // stream offset of the next byte
	connStats
}

func (s *sink) Write(p []byte) (int, error) {
	if s.check {
		if err := verify(p, s.off); err != nil {
			return 0, &integrityError{s.conn, err.Error()}
		}
	}
	s.off += uint64(len(p))
	if s.start.IsZero() {
		s.start = time.Now()
	} else {
		s.records++
		s.bytes += uint64(len(p))
	}
	return len(p), nil
}

// consume delivers connection index's stream to a sink until the producer's
// Close ends it with io.EOF, which ends the window: with -io copy by
// io.Copy, with rw by reading each o.size-byte message into one buffer.
func consume(c net.Conn, index int, o options) (connStats, error) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	s := &sink{conn: index, check: o.mode == "throughput"}
	var err error
	if o.io == "rw" {
		err = readMessages(c, s, o.size)
	} else {
		// WriteTo hands each ring record's slice to s.Write; nothing is copied.
		_, err = io.Copy(s, c)
	}
	if err != nil {
		var ie *integrityError
		if errors.As(err, &ie) {
			return s.connStats, err
		}
		return s.connStats, fmt.Errorf("connection %d: %w", index, err)
	}
	s.end = time.Now()
	return s.connStats, nil
}

// readMessages reads c as size-byte messages into one buffer, each handed
// to s, until io.EOF; a short last message is a truncated stream.
func readMessages(c net.Conn, s *sink, size int) error {
	buf := make([]byte, size)
	for seq := uint64(1); ; seq++ {
		n, err := io.ReadFull(c, buf)
		switch err {
		case nil:
		case io.EOF:
			return nil
		case io.ErrUnexpectedEOF:
			return &integrityError{s.conn, fmt.Sprintf("message %d is %d bytes, want %d", seq, n, size)}
		default:
			return err
		}
		if _, err := s.Write(buf); err != nil {
			return err
		}
	}
}

// measureLatency writes size-byte records on c and times each echo for d
// after a warm-up, whose round trips are discarded.
func measureLatency(c net.Conn, size int, d time.Duration) (*latency, error) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	const warmup = 1000
	msg, reply := newTemplate(size), make([]byte, size)
	samples := make([]time.Duration, 0, 1<<20)
	var start int64
	for seq := uint64(1); ; seq++ {
		t0 := nowNanos()
		if seq == warmup+1 {
			start = t0 // the window opens when the warm-up ends
		}
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
			return nil, &integrityError{0, fmt.Sprintf("echo of record %d differs from the record sent", seq)}
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
