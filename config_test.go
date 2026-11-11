package ipc

import (
	"testing"
	"time"
)

func TestSpinDurationSpinsByDefaultOnlyWithSeveralCPUs(t *testing.T) {
	saved := numCPU
	t.Cleanup(func() { numCPU = saved })
	for name, tc := range map[string]struct {
		cpus       int
		spin, want time.Duration
	}{
		"1 CPU, zero parks at once":       {1, 0, 0},
		"1 CPU, positive spins as given":  {1, 20 * time.Microsecond, 20 * time.Microsecond},
		"1 CPU, negative parks at once":   {1, -1, 0},
		"2 CPUs, zero spins 50µs":         {2, 0, 50 * time.Microsecond},
		"2 CPUs, positive spins as given": {2, 20 * time.Microsecond, 20 * time.Microsecond},
		"2 CPUs, negative parks at once":  {2, -1, 0},
	} {
		t.Run(name, func(t *testing.T) {
			numCPU = func() int { return tc.cpus }
			if got := spinDuration(tc.spin); got != tc.want {
				t.Fatalf("spinDuration(%v) with NumCPU %d = %v; want %v", tc.spin, tc.cpus, got, tc.want)
			}
		})
	}
}
