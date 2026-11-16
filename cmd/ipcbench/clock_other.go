//go:build !windows

package main

import "time"

var clockBase = time.Now()

// now returns the monotonic time since process start.
func now() time.Duration {
	return time.Since(clockBase)
}
