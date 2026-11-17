package main

import (
	"testing"
	"time"
)

func TestClockIsMonotonic(t *testing.T) {
	prev := now()
	for i := range 10000 {
		next := now()
		if next < prev {
			t.Fatalf("read %d went backwards: %d after %d", i, next, prev)
		}
		prev = next
	}
}

// A coarse clock (Windows time.Now ticks in 0.5–15.6 ms) never shows a
// positive step this small, so µs round trips would measure as 0.
func TestClockResolutionIsAtMostTenMicroseconds(t *testing.T) {
	const limit = 10 * time.Microsecond
	smallest := time.Duration(-1)
	steps := 0
	prev := now()
	for i := 0; i < 5_000_000 && steps < 1000; i++ {
		next := now()
		if d := next - prev; d > 0 {
			steps++
			if smallest < 0 || d < smallest {
				smallest = d
			}
		}
		prev = next
	}
	if smallest < 0 {
		t.Fatal("clock never advanced")
	}
	if smallest > limit {
		t.Fatalf("smallest positive step %v, want <= %v", smallest, limit)
	}
}
