//go:build !windows

package main

import "time"

var clockBase = time.Now()

// nowNanos returns monotonic nanoseconds since process start.
func nowNanos() int64 {
	return int64(time.Since(clockBase))
}
