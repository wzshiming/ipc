package ipc

import (
	"errors"
	"fmt"
	"io"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// The benchmark set measures every hot path the performance plan targets,
// in process; later changes are judged by these names.
//
//	WriteOnly/64B, ReadOnly/64B/{msg,buf32K}, ReadFromOnly/64B: one end, one goroutine
//	Stream/64B/{free,writer-bound,reader-bound}, PingPong/64B/{spin,parked}: two locked threads
//	Bulk/{64KiB,1MiB}/{rw,copy}/ring={256KiB,1MiB,4MiB,16MiB}: large records
//	Setup/{serial,parallel16}: Dial + Accept + Close
//
// internal/ring keeps BenchmarkProtocol64B and BenchmarkFill64KiB as the
// A/A noise floor. Every benchmark reports the time per record, message or
// connection and its allocations; the Stream ones add the records a Read
// returned on average, which shows the state they ran in.

// msgWork is the LCG steps a bound end spends per message, about three
// times the cost of moving one, so the other end is the one that waits.
const msgWork = 64

// workSink keeps the results of lcg observable.
var workSink atomic.Uint64

// lcg advances x by n steps of a linear congruential generator: fixed CPU
// work on a dependency chain the compiler cannot shorten.
func lcg(x uint64, n int) uint64 {
	for range n {
		x = x*6364136223846793005 + 1442695040888963407
	}
	return x
}

// fill64B fills the empty ring from tx with 64 B records and drains it on
// rx, touching every page, and returns how many Write puts in without
// waiting for room from any position: one less than fit here, as the wrap
// marker may cost a record elsewhere.
func fill64B(b *testing.B, tx, rx *Conn) int {
	n := 0
	for {
		_, err := tx.TryReserve(64)
		if errors.Is(err, ErrWouldBlock) {
			break
		}
		if err != nil {
			b.Fatal(err)
		}
		if err := tx.Commit(); err != nil {
			b.Fatal(err)
		}
		n++
	}
	drain(b, rx, make([]byte, 32<<10), int64(n)*64)
	return n - 1
}

// source is an io.Reader handing out left bytes from a template in reads of
// at most chunk bytes, then io.EOF.
type source struct {
	template []byte
	chunk    int
	left     int64
}

func (s *source) Read(p []byte) (int, error) {
	if s.left == 0 {
		return 0, io.EOF
	}
	n := min(len(p), s.chunk)
	if int64(n) > s.left {
		n = int(s.left)
	}
	s.left -= int64(n)
	return copy(p, s.template[:n]), nil
}

var errQuota = errors.New("quota reached")

// quota is an io.Writer that takes left bytes, then ends the WriteTo feeding
// it with errQuota.
type quota struct{ left int64 }

func (q *quota) Write(p []byte) (int, error) {
	q.left -= int64(len(p))
	if q.left <= 0 {
		return len(p), errQuota
	}
	return len(p), nil
}

// BenchmarkWriteOnly writes 64 B messages into an empty ring until it is
// nearly full, the drain untimed.
func BenchmarkWriteOnly(b *testing.B) {
	b.Run("64B", func(b *testing.B) {
		tx, rx := pair(b, ListenConfig{}, Dialer{})
		batch := fill64B(b, tx, rx)
		msg, buf := make([]byte, 64), make([]byte, 32<<10)
		b.ReportAllocs()
		b.ResetTimer()
		for done := 0; done < b.N; {
			n := min(batch, b.N-done)
			for range n {
				if _, err := tx.Write(msg); err != nil {
					b.Fatal(err)
				}
			}
			done += n
			b.StopTimer()
			drain(b, rx, buf, int64(n)*64)
			b.StartTimer()
		}
	})
}

// BenchmarkReadOnly reads 64 B records written beforehand, into a buffer of
// one message or of 32 KiB.
func BenchmarkReadOnly(b *testing.B) {
	b.Run("64B", func(b *testing.B) {
		for _, c := range []struct {
			name string
			size int
		}{{"msg", 64}, {"buf32K", 32 << 10}} {
			b.Run(c.name, func(b *testing.B) {
				tx, rx := pair(b, ListenConfig{}, Dialer{})
				batch := fill64B(b, tx, rx)
				msg, buf := make([]byte, 64), make([]byte, c.size)
				b.ReportAllocs()
				b.ResetTimer()
				for done := 0; done < b.N; {
					n := min(batch, b.N-done)
					b.StopTimer()
					for range n {
						if _, err := tx.Write(msg); err != nil {
							b.Fatal(err)
						}
					}
					b.StartTimer()
					drain(b, rx, buf, int64(n)*64)
					done += n
				}
			})
		}
	})
}

// BenchmarkReadFromOnly feeds ReadFrom from a source that hands out 64 B
// per Read into an empty ring, the drain untimed. ReadFrom reserves the free
// run, which a wrap may halve, and wants 4 KiB of it, so a quarter of the
// ring's 64 B records per batch never waits for room.
func BenchmarkReadFromOnly(b *testing.B) {
	b.Run("64B", func(b *testing.B) {
		tx, rx := pair(b, ListenConfig{}, Dialer{})
		batch := fill64B(b, tx, rx) / 4
		src := &source{template: make([]byte, 64), chunk: 64}
		buf := make([]byte, 32<<10)
		b.ReportAllocs()
		b.ResetTimer()
		for done := 0; done < b.N; {
			n := min(batch, b.N-done)
			src.left = int64(n) * 64
			if k, err := tx.ReadFrom(src); err != nil || k != int64(n)*64 {
				b.Fatalf("ReadFrom = %d, %v; want %d, nil", k, err, int64(n)*64)
			}
			done += n
			b.StopTimer()
			drain(b, rx, buf, int64(n)*64)
			b.StartTimer()
		}
	})
}

// BenchmarkStream moves 64 B messages between two goroutines on locked OS
// threads, the reader using a 32 KiB buffer: free, with no work on either
// end; writer-bound, with msgWork per message on the writer, so the reader
// waits for data; reader-bound, with it on the reader, so the writer waits
// for room. rec/read is how many records a Read returned on average.
func BenchmarkStream(b *testing.B) {
	b.Run("64B", func(b *testing.B) {
		b.Run("free", func(b *testing.B) { stream(b, 0, 0) })
		b.Run("writer-bound", func(b *testing.B) { stream(b, msgWork, 0) })
		b.Run("reader-bound", func(b *testing.B) { stream(b, 0, msgWork) })
	})
}

// stream sends b.N messages, the ends spending writerWork and readerWork
// LCG steps per message.
func stream(b *testing.B, writerWork, readerWork int) {
	tx, rx := pair(b, ListenConfig{}, Dialer{})
	msg, buf := make([]byte, 64), make([]byte, 32<<10)
	b.ReportAllocs()
	b.ResetTimer()
	done := make(chan error, 1)
	go func() {
		runtime.LockOSThread()
		defer runtime.UnlockOSThread()
		var acc uint64
		for range b.N {
			acc = lcg(acc, writerWork)
			if _, err := tx.Write(msg); err != nil {
				done <- err
				return
			}
		}
		workSink.Add(acc)
		done <- nil
	}()
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	var acc uint64
	reads := 0
	for got := int64(0); got < int64(b.N)*64; reads++ {
		n, err := rx.Read(buf)
		if err != nil {
			b.Fatal(err)
		}
		got += int64(n)
		for range n / 64 {
			acc = lcg(acc, readerWork)
		}
	}
	workSink.Add(acc)
	if err := <-done; err != nil {
		b.Fatal(err)
	}
	b.ReportMetric(float64(b.N)/float64(reads), "rec/read")
}

// BenchmarkPingPong times 64 B round trips between two goroutines on locked
// OS threads: spin with the default Spin, parked with Spin negative on both
// ends so every hop goes through the doorbell.
func BenchmarkPingPong(b *testing.B) {
	b.Run("64B", func(b *testing.B) {
		b.Run("spin", func(b *testing.B) { pingPong(b, 0) })
		b.Run("parked", func(b *testing.B) { pingPong(b, -1) })
	})
}

// pingPong runs b.N round trips against echo on a connection with spin.
func pingPong(b *testing.B, spin time.Duration) {
	lc, dc := pair(b, ListenConfig{Spin: spin}, Dialer{Spin: spin})
	msg, buf := make([]byte, 64), make([]byte, 64)
	b.ReportAllocs()
	b.ResetTimer()
	done := echo(dc, b.N)
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	for range b.N {
		if _, err := lc.Write(msg); err != nil {
			b.Fatal(err)
		}
		if _, err := lc.Read(buf); err != nil {
			b.Fatal(err)
		}
	}
	if err := <-done; err != nil {
		b.Fatal(err)
	}
}

// BenchmarkBulk moves 64 KiB and 1 MiB messages over rings of 256 KiB to
// 16 MiB: rw with Write and Read into a buffer of one message, copy with
// ReadFrom and WriteTo, the source filling each record from a template and
// the sink only counting what WriteTo hands it, as io.Copy between a
// producer and a consumer reading in place would.
func BenchmarkBulk(b *testing.B) {
	for _, size := range []int{64 << 10, 1 << 20} {
		b.Run(sizeName(size), func(b *testing.B) {
			for _, path := range []struct {
				name string
				run  func(b *testing.B, size, ringSize int)
			}{{"rw", bulkRW}, {"copy", bulkCopy}} {
				b.Run(path.name, func(b *testing.B) {
					for _, ringSize := range []int{256 << 10, 1 << 20, 4 << 20, 16 << 20} {
						b.Run("ring="+sizeName(ringSize), func(b *testing.B) {
							path.run(b, size, ringSize)
						})
					}
				})
			}
		})
	}
}

// sizeName names a power-of-two byte count as the benchmarks do.
func sizeName(n int) string {
	if n >= 1<<20 {
		return fmt.Sprintf("%dMiB", n>>20)
	}
	return fmt.Sprintf("%dKiB", n>>10)
}

// bulkRW moves b.N messages of size bytes with Write and Read.
func bulkRW(b *testing.B, size, ringSize int) {
	tx, rx := pair(b, ListenConfig{RingSize: ringSize}, Dialer{RingSize: ringSize})
	msg, buf := make([]byte, size), make([]byte, size)
	b.SetBytes(int64(size))
	b.ReportAllocs()
	b.ResetTimer()
	writeRead(b, tx, rx, msg, buf, b.N)
}

// bulkCopy moves b.N messages of size bytes with ReadFrom and WriteTo.
func bulkCopy(b *testing.B, size, ringSize int) {
	tx, rx := pair(b, ListenConfig{RingSize: ringSize}, Dialer{RingSize: ringSize})
	total := int64(size) * int64(b.N)
	src := &source{template: make([]byte, size), chunk: size, left: total}
	sink := &quota{left: total}
	b.SetBytes(int64(size))
	b.ReportAllocs()
	b.ResetTimer()
	done := make(chan error, 1)
	go func() {
		n, err := tx.ReadFrom(src)
		if err == nil && n != total {
			err = fmt.Errorf("ReadFrom sent %d bytes; want %d", n, total)
		}
		done <- err
	}()
	n, err := rx.WriteTo(sink)
	if !errors.Is(err, errQuota) || n != total {
		b.Fatalf("WriteTo = %d, %v; want %d, %v", n, err, total, errQuota)
	}
	if err := <-done; err != nil {
		b.Fatal(err)
	}
}

// BenchmarkSetup makes connections, one dialer at a time or 16 at once:
// each is a Dial, an Accept, the dialer's Close and the listener side's
// once it sees that close.
func BenchmarkSetup(b *testing.B) {
	b.Run("serial", func(b *testing.B) { setup(b, 1) })
	b.Run("parallel16", func(b *testing.B) { setup(b, 16) })
}

// setup makes b.N connections with dialers dialing at once, one accept
// loop serving them until their dialer's close.
func setup(b *testing.B, dialers int) {
	l, path := listen(b, ListenConfig{})
	d := Dialer{Timeout: 10 * time.Second}
	b.ReportAllocs()
	b.ResetTimer()
	var served sync.WaitGroup
	acceptErr := make(chan error, 1)
	served.Add(1)
	go func() {
		defer served.Done()
		for range b.N {
			c, err := l.Accept()
			if err != nil {
				acceptErr <- err
				return
			}
			served.Add(1)
			go func() {
				defer served.Done()
				io.Copy(io.Discard, c)
				c.Close()
			}()
		}
	}()
	var dialed sync.WaitGroup
	dialErr := make(chan error, dialers)
	for i := range dialers {
		n := b.N / dialers
		if i < b.N%dialers {
			n++
		}
		dialed.Add(1)
		go func() {
			defer dialed.Done()
			for range n {
				c, err := d.Dial(path)
				if err != nil {
					dialErr <- err
					return
				}
				c.Close()
			}
		}()
	}
	dialed.Wait()
	select {
	case err := <-dialErr:
		b.Fatal(err)
	default:
	}
	served.Wait()
	select {
	case err := <-acceptErr:
		b.Fatal(err)
	default:
	}
}
