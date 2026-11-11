//go:build !darwin && !linux && !windows

package control

// pathLock is never taken here: without shared memory to serve, Listen keeps
// its lock-free behaviour on these systems.
type pathLock struct{}

func lockPath(string) (*pathLock, error) { return nil, nil }

func (*pathLock) unlock() {}
