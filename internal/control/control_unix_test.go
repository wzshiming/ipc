//go:build darwin || linux

package control

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"
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

// dirNames returns the names in the directory of path.
func dirNames(t *testing.T, path string) []string {
	t.Helper()
	ents, err := os.ReadDir(filepath.Dir(path))
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, e := range ents {
		names = append(names, e.Name())
	}
	return names
}

func TestListenMakesNoFileBesideItsSocketFileAndCloseLeavesNone(t *testing.T) {
	path := sockPath(t)
	l := listen(t, path)
	if got := dirNames(t, path); len(got) != 1 || got[0] != filepath.Base(path) {
		t.Fatalf("files while listening: %q; want only the socket file %q", got, filepath.Base(path))
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	if got := dirNames(t, path); len(got) != 0 {
		t.Fatalf("files after Close: %q; want none", got)
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

func TestListenOnALiveDatagramSocketIsAddrInUseAndKeepsItsFile(t *testing.T) {
	path := sockPath(t)
	dg, err := net.ListenUnixgram("unixgram", &net.UnixAddr{Name: path, Net: "unixgram"})
	if err != nil {
		t.Fatal(err)
	}
	defer dg.Close()
	before, err := os.Lstat(path)
	if err != nil {
		t.Fatal(err)
	}
	if l, err := Listen(t.Context(), path); !isAddrInUse(err) {
		if l != nil {
			l.Close()
		}
		t.Fatalf("Listen on a live datagram socket: %v, want EADDRINUSE", err)
	}
	if after, err := os.Lstat(path); err != nil || !os.SameFile(before, after) {
		t.Fatalf("the datagram socket's file did not stay: %v", err)
	}
}

func TestConcurrentListenOnAFreePathLeavesOneListener(t *testing.T) {
	const trials, starters = 200, 8
	var several, none int
	for range trials {
		path := sockPath(t)
		var mu sync.Mutex
		var live []*Listener
		var wg sync.WaitGroup
		start := make(chan struct{})
		for range starters {
			wg.Go(func() {
				<-start
				if l, err := Listen(t.Context(), path); err == nil {
					mu.Lock()
					live = append(live, l)
					mu.Unlock()
				}
			})
		}
		close(start)
		wg.Wait()
		switch {
		case len(live) == 0:
			none++
		case len(live) > 1:
			several++
		}
		for _, l := range live {
			l.Close()
		}
	}
	if several+none > 0 {
		t.Fatalf("%d trials of %d concurrent Listen calls on a free path: %d left several Listeners, %d none",
			trials, starters, several, none)
	}
}

func TestListenWaitsForALockedDirectoryUntilCtxEnds(t *testing.T) {
	path := sockPath(t)
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		t.Fatal(err)
	}
	defer dir.Close()
	if err := unix.Flock(int(dir.Fd()), unix.LOCK_EX); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	if l, err := Listen(ctx, path); !errors.Is(err, context.DeadlineExceeded) {
		if l != nil {
			l.Close()
		}
		t.Fatalf("Listen while the directory is locked: %v, want context.DeadlineExceeded", err)
	}
	if got := dirNames(t, path); len(got) != 0 {
		t.Fatalf("files after Listen gave up: %q; want none", got)
	}
	if err := unix.Flock(int(dir.Fd()), unix.LOCK_UN); err != nil {
		t.Fatal(err)
	}
	listen(t, path)
}

func TestListenInADirectoryItCannotReadServesAndLeavesNothing(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root can read any directory")
	}
	path := sockPath(t)
	dir := filepath.Dir(path)
	if err := os.Chmod(dir, 0o300); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Chmod(dir, 0o700) })
	l := listen(t, path)
	acceptOne(t, l, path)
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	if got := dirNames(t, path); len(got) != 0 {
		t.Fatalf("files after Close: %q; want none", got)
	}
}

func TestListenUnderAFIFOFailsAtOnceAndCreatesNothing(t *testing.T) {
	fifo := filepath.Join(filepath.Dir(sockPath(t)), "fifo")
	if err := unix.Mkfifo(fifo, 0o600); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		l, err := Listen(t.Context(), filepath.Join(fifo, "s.sock"))
		if l != nil {
			l.Close()
		}
		done <- err
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("Listen under a FIFO succeeded")
		}
	case <-time.After(time.Second):
		// A writer ends an open that waits on the FIFO for one.
		if w, err := os.OpenFile(fifo, os.O_WRONLY|unix.O_NONBLOCK, 0); err == nil {
			w.Close()
		}
		t.Fatal("Listen under a FIFO did not return within 1 s")
	}
	if got := dirNames(t, fifo); len(got) != 1 || got[0] != "fifo" {
		t.Fatalf("files after Listen: %q; want only the FIFO", got)
	}
}
