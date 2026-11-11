//go:build darwin || linux

package control

import (
	"errors"
	"net"
	"os"
	"syscall"
	"testing"
	"time"
)

func isAddrInUse(err error) bool { return errors.Is(err, syscall.EADDRINUSE) }

func TestListenSocketFileIsOwnerOnly(t *testing.T) {
	path := sockPath(t)
	listen(t, path)
	fi, err := os.Lstat(path)
	if err != nil {
		t.Fatal(err)
	}
	if perm := fi.Mode().Perm(); perm != 0o600 {
		t.Fatalf("socket file mode %v, want 0600", perm)
	}
}

func TestListenerHoldsAnOwnerOnlyLockFileUntilClose(t *testing.T) {
	path := sockPath(t)
	l := listen(t, path)
	fi, err := os.Lstat(path + ".lock")
	if err != nil {
		t.Fatalf("no lock file while listening: %v", err)
	}
	if !fi.Mode().IsRegular() || fi.Mode().Perm() != 0o600 {
		t.Fatalf("lock file mode %v, want a regular file with 0600", fi.Mode())
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(path + ".lock"); !os.IsNotExist(err) {
		t.Fatalf("lock file left after Close: %v", err)
	}
}

func TestListenOnALiveListenerWithAFullAcceptQueueIsAddrInUse(t *testing.T) {
	path := sockPath(t)
	l := listen(t, path)
	var queued []net.Conn
	defer func() {
		for _, c := range queued {
			c.Close()
		}
	}()
	// A dial fails once the queue is full: refused on darwin, EAGAIN on linux.
	for {
		c, err := net.DialTimeout("unix", path, time.Second)
		if err != nil {
			break
		}
		queued = append(queued, c)
		if len(queued) == 1<<14 {
			t.Skip("the accept queue did not fill")
		}
	}
	l2, err := Listen(t.Context(), path)
	if err == nil {
		l2.Close()
		t.Fatalf("second Listen replaced a live listener with %d connections queued", len(queued))
	}
	if !isAddrInUse(err) {
		t.Fatalf("got %v, want EADDRINUSE", err)
	}
	for _, c := range queued {
		c.Close()
		k, err := l.Accept()
		if err != nil {
			t.Fatal(err)
		}
		k.Close()
	}
	acceptOne(t, l, path)
}
