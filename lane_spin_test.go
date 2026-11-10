package ipc

import (
	"runtime"
	"slices"
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
			n, err := dc.Lane(0).Read(buf)
			if err == nil {
				_, err = dc.Lane(0).Write(buf[:n])
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
		if _, err := lc.Lane(0).Write(msg); err != nil {
			tb.Fatal(err)
		}
		if _, err := lc.Lane(0).Read(buf); err != nil {
			tb.Fatal(err)
		}
		samples = append(samples, time.Since(t0))
	}
	if err := <-done; err != nil {
		tb.Fatal(err)
	}
	return samples
}

func TestPinnedPingPongDoesNotPayThreadHandoffPerRoundTrip(t *testing.T) {
	if raceEnabled() || testing.Short() || runtime.GOMAXPROCS(0) < 2 {
		t.Skip("needs two idle CPUs and no race detector")
	}
	lc, dc := pair(t, ListenConfig{}, Dialer{})
	pinnedPingPong(t, lc, dc, 1000)
	samples := pinnedPingPong(t, lc, dc, 5000)
	slices.Sort(samples)
	if p50 := samples[len(samples)/2]; p50 > 8*time.Microsecond {
		t.Fatalf("pinned 64 B round trip p50 = %v; want ≤ 8µs", p50)
	}
}

func BenchmarkPinnedPingPong64B(b *testing.B) {
	lc, dc := pair(b, ListenConfig{}, Dialer{})
	b.ResetTimer()
	pinnedPingPong(b, lc, dc, b.N)
}
