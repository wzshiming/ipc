package ring

import (
	"encoding/binary"
	"runtime"
	"testing"
)

// runPair runs produce on its own locked OS thread and consume on the
// benchmark goroutine's locked thread; both spin on ErrFull/ErrEmpty.
func runPair(b *testing.B, produce, consume func()) {
	done := make(chan struct{})
	b.ReportAllocs()
	b.ResetTimer()
	go func() {
		runtime.LockOSThread()
		defer runtime.UnlockOSThread()
		produce()
		close(done)
	}()
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	consume()
	<-done
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "rec/s")
}

// BenchmarkProtocol64B measures the ring protocol alone: 64-byte records,
// payload neither written nor read.
func BenchmarkProtocol64B(b *testing.B) {
	p, c := newPair(b, newRegion(b, 4<<20))
	b.SetBytes(64)
	runPair(b, func() {
		for i := 0; i < b.N; {
			if _, err := p.TryReserve(64); err != nil {
				continue
			}
			p.Commit()
			i++
		}
	}, func() {
		for i := 0; i < b.N; {
			if _, err := c.TryNext(); err != nil {
				continue
			}
			c.Release()
			i++
		}
	})
}

// BenchmarkFill64KiB writes every byte of each 64 KiB record from a template
// and reads one word every 128 bytes on the consumer side.
func BenchmarkFill64KiB(b *testing.B) {
	const size = 64 << 10
	p, c := newPair(b, newRegion(b, 8<<20))
	template := make([]byte, size)
	for i := range template {
		template[i] = byte(i)
	}
	var sum uint64
	b.SetBytes(size)
	runPair(b, func() {
		for i := 0; i < b.N; {
			buf, err := p.TryReserve(size)
			if err != nil {
				continue
			}
			copy(buf, template)
			p.Commit()
			i++
		}
	}, func() {
		for i := 0; i < b.N; {
			buf, err := c.TryNext()
			if err != nil {
				continue
			}
			for j := 0; j < len(buf); j += 128 {
				sum += binary.LittleEndian.Uint64(buf[j:])
			}
			c.Release()
			i++
		}
	})
	if sum == 1 {
		b.Log(sum)
	}
}
