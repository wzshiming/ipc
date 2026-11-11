package ipc

import (
	"runtime"
	"testing"
	"time"
)

// pinnedPingPong runs rounds 64 B round trips between two goroutines locked
// to OS threads and returns each round trip's duration.
func pinnedPingPong(tb testing.TB, lc, dc *Conn, rounds int) []time.Duration {
	done := make(chan error, 1)
	go func() {
		runtime.LockOSThread()
		defer runtime.UnlockOSThread()
		buf := make([]byte, 64)
		for range rounds {
			n, err := dc.Read(buf)
			if err == nil {
				_, err = dc.Write(buf[:n])
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
	samples := make([]time.Duration, 0, rounds)
	for range rounds {
		t0 := time.Now()
		if _, err := lc.Write(msg); err != nil {
			tb.Fatal(err)
		}
		if _, err := lc.Read(buf); err != nil {
			tb.Fatal(err)
		}
		samples = append(samples, time.Since(t0))
	}
	if err := <-done; err != nil {
		tb.Fatal(err)
	}
	return samples
}

func BenchmarkPinnedPingPong64B(b *testing.B) {
	lc, dc := pair(b, ListenConfig{}, Dialer{})
	b.ResetTimer()
	pinnedPingPong(b, lc, dc, b.N)
}
