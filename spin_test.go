package ipc

import (
	"runtime"
	"slices"
	"testing"
	"time"
)

// pinnedPingPong runs rounds 64 B round trips, each end locked to an OS thread.
func pinnedPingPong(tb testing.TB, lc, dc *Conn, rounds int, deadlineEachCall, timed bool) []time.Duration {
	read := func(c *Conn, p []byte) (int, error) {
		if deadlineEachCall {
			c.SetReadDeadline(time.Now().Add(time.Minute))
		}
		return c.Read(p)
	}
	write := func(c *Conn, p []byte) (int, error) {
		if deadlineEachCall {
			c.SetWriteDeadline(time.Now().Add(time.Minute))
		}
		return c.Write(p)
	}
	done := make(chan error, 1)
	go func() {
		runtime.LockOSThread()
		defer runtime.UnlockOSThread()
		buf := make([]byte, 64)
		for range rounds {
			n, err := read(dc, buf)
			if err == nil {
				_, err = write(dc, buf[:n])
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
	var samples []time.Duration
	if timed {
		samples = make([]time.Duration, 0, rounds)
	}
	for range rounds {
		var t0 time.Time
		if timed {
			t0 = time.Now()
		}
		if _, err := write(lc, msg); err != nil {
			tb.Fatal(err)
		}
		if _, err := read(lc, buf); err != nil {
			tb.Fatal(err)
		}
		if timed {
			samples = append(samples, time.Since(t0))
		}
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
	pinnedPingPong(t, lc, dc, 1000, false, false)
	samples := pinnedPingPong(t, lc, dc, 5000, false, true)
	slices.Sort(samples)
	if p50 := samples[len(samples)/2]; p50 > 8*time.Microsecond {
		t.Fatalf("pinned 64 B round trip p50 = %v; want ≤ 8µs", p50)
	}
}

func BenchmarkPinnedPingPong64B(b *testing.B) {
	lc, dc := pair(b, ListenConfig{}, Dialer{})
	b.ResetTimer()
	// Timed though the samples go unused, keeping ns/op comparable with earlier runs.
	pinnedPingPong(b, lc, dc, b.N, false, true)
}
