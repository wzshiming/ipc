package main

import "time"

// elapsed returns the interval between two nowNanos readings.
func elapsed(a, b int64) time.Duration {
	return time.Duration(b - a)
}
