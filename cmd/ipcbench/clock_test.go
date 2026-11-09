package main

import (
	"testing"
	"time"
)

func TestClockIsMonotonic(t *testing.T) {
	prev := nowNanos()
	for i := range 10000 {
		now := nowNanos()
		if now < prev {
			t.Fatalf("read %d went backwards: %d after %d", i, now, prev)
		}
		prev = now
	}
}

// A coarse clock (Windows time.Now ticks in 0.5–15.6 ms) never shows a
// positive step this small, so µs round trips would measure as 0.
func TestClockResolutionIsAtMostTenMicroseconds(t *testing.T) {
	const limit = 10 * time.Microsecond
	smallest := time.Duration(-1)
	steps := 0
	prev := nowNanos()
	for i := 0; i < 5_000_000 && steps < 1000; i++ {
		now := nowNanos()
		if d := elapsed(prev, now); d > 0 {
			steps++
			if smallest < 0 || d < smallest {
				smallest = d
			}
		}
		prev = now
	}
	if smallest < 0 {
		t.Fatal("clock never advanced")
	}
	if smallest > limit {
		t.Fatalf("smallest positive step %v, want <= %v", smallest, limit)
	}
}
