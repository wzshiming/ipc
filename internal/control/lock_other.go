//go:build !darwin && !linux && !windows

package control

import "context"

// pathLock is never taken here: without shared memory to serve, Listen keeps
// its lock-free behaviour on these systems.
type pathLock struct{}

func lockPath(context.Context, string) (*pathLock, error) { return nil, nil }

func (*pathLock) keep() *pathLock { return nil }

func (*pathLock) unlock() {}
