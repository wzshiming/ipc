//go:build !darwin && !linux && !windows

package shm

type segSys struct{}

func (*segSys) release([]byte) error { return nil }

// Create returns ErrUnsupported on this platform.
func Create(size int) (*Segment, error) { return nil, ErrUnsupported }
