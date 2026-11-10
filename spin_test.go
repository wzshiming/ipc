package ipc

import (
	"runtime"
	"slices"
	"testing"
	"time"
)

// echo answers rounds messages of up to 64 B on c from a goroutine locked to
// an OS thread; the channel reports its first error, or nil once done.
func echo(c *Conn, rounds int) <-chan error {
	done := make(chan error, 1)
	go func() {
		runtime.LockOSThread()
		defer runtime.UnlockOSThread()
		buf := make([]byte, 64)
		for range rounds {
			n, err := c.Read(buf)
			if err == nil {
				_, err = c.Write(buf[:n])
			}
			if err != nil {
				done <- err
				return
			}
		}
		done <- nil
	}()
	return done
}

// pinnedPingPong runs rounds 64 B round trips between two goroutines locked
// to OS threads and returns each round trip's duration.
func pinnedPingPong(tb testing.TB, lc, dc *Conn, rounds int) []time.Duration {
	done := echo(dc, rounds)
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
