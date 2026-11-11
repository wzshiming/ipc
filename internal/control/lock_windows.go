//go:build windows

package control

import (
	"context"
	"os"

	"golang.org/x/sys/windows"
)

// pathLock is a LockFileEx lock on the file beside a socket path; Windows
// drops it when the process dies.
type pathLock struct{ f *os.File }

// lockPath takes the lock file of the socket at path. While another Listener
// holds it, it fails with WSAEADDRINUSE, as a bind on that live socket would.
func lockPath(_ context.Context, path string) (*pathLock, error) {
	f, err := os.OpenFile(path+".lock", os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, err
	}
	err = windows.LockFileEx(windows.Handle(f.Fd()), windows.LOCKFILE_EXCLUSIVE_LOCK|windows.LOCKFILE_FAIL_IMMEDIATELY, 0, 1, 0, new(windows.Overlapped))
	if err == nil {
		return &pathLock{f: f}, nil
	}
	f.Close()
	if err == windows.ERROR_LOCK_VIOLATION {
		return nil, os.NewSyscallError("bind", windows.WSAEADDRINUSE)
	}
	return nil, os.NewSyscallError("LockFileEx", err)
}

// keep returns k: a Listener holds the lock file until Close.
func (k *pathLock) keep() *pathLock { return k }

// unlock releases the lock and leaves the file, which Windows cannot delete
// while another Listen has it open.
func (k *pathLock) unlock() {
	if k == nil {
		return
	}
	windows.UnlockFileEx(windows.Handle(k.f.Fd()), 0, 1, 0, new(windows.Overlapped))
	k.f.Close()
}
