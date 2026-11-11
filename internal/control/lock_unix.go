//go:build darwin || linux

package control

import (
	"context"
	"os"
	"path/filepath"
	"time"

	"golang.org/x/sys/unix"
)

// pathLock is a flock on the directory of a socket path, which Listen holds
// while it binds; the kernel drops it when the process dies.
type pathLock struct{ dir *os.File }

// lockPath locks the directory of the socket at path, waiting until ctx ends
// while another holds it. A directory it cannot open for reading or lock is
// left unlocked, and Listen goes ahead without the lock.
func lockPath(ctx context.Context, path string) (*pathLock, error) {
	for wait := 50 * time.Microsecond; ; wait = min(2*wait, 10*time.Millisecond) {
		k, err := tryLockDir(filepath.Dir(path))
		if err != unix.EWOULDBLOCK {
			return k, nil
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(wait):
		}
	}
}

// tryLockDir opens the directory name and locks it without waiting.
func tryLockDir(name string) (*pathLock, error) {
	// O_DIRECTORY fails a non-directory at once, where a FIFO's open would block.
	dir, err := os.OpenFile(name, os.O_RDONLY|unix.O_DIRECTORY, 0)
	if err != nil {
		return nil, err
	}
	if err := unix.Flock(int(dir.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		dir.Close()
		return nil, err
	}
	return &pathLock{dir: dir}, nil
}

// keep returns what of k a Listener holds until Close: nothing, as the
// directory stays locked only while Listen binds.
func (k *pathLock) keep() *pathLock {
	k.unlock()
	return nil
}

// unlock closes the directory, which drops its flock.
func (k *pathLock) unlock() {
	if k != nil {
		k.dir.Close()
	}
}
