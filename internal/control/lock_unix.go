//go:build darwin || linux

package control

import (
	"os"

	"golang.org/x/sys/unix"
)

// pathLock is a flock on the file beside a socket path; the kernel drops it
// when the process dies.
type pathLock struct {
	f    *os.File
	name string
}

// lockPath takes the lock file of the socket at path. While another Listener
// holds it, it fails with EADDRINUSE, as a bind on that live socket would.
func lockPath(path string) (*pathLock, error) {
	name := path + ".lock"
	for {
		f, err := os.OpenFile(name, os.O_RDWR|os.O_CREATE|unix.O_NOFOLLOW, 0o600)
		if err != nil {
			return nil, err
		}
		err = unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB)
		if err == nil && linked(f, name) {
			return &pathLock{f: f, name: name}, nil
		}
		f.Close()
		if err == unix.EWOULDBLOCK {
			return nil, os.NewSyscallError("bind", unix.EADDRINUSE)
		}
		if err != nil {
			return nil, os.NewSyscallError("flock", err)
		}
		// Its holder removed the file before we locked it; lock the new one.
	}
}

// linked reports whether f is still the file at name.
func linked(f *os.File, name string) bool {
	fi, err := f.Stat()
	if err != nil {
		return false
	}
	at, err := os.Lstat(name)
	return err == nil && os.SameFile(fi, at)
}

// unlock removes the lock file while it is still the locked one, which
// lockPath's check makes safe, then releases the lock.
func (k *pathLock) unlock() {
	if k == nil {
		return
	}
	if linked(k.f, k.name) {
		os.Remove(k.name)
	}
	k.f.Close()
}
