//go:build darwin || linux || windows

package control

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// envHold makes the test binary a process that listens on the path it names
// and holds the Listener until killed.
const envHold = "IPC_CONTROL_TEST_HOLD"

func TestMain(m *testing.M) {
	if path := os.Getenv(envHold); path != "" {
		if err := hold(path); err != nil {
			fmt.Fprintln(os.Stderr, "holder:", err)
			os.Exit(1)
		}
		os.Exit(0)
	}
	os.Exit(m.Run())
}

// hold listens on path, says so on stdout and keeps the Listener until stdin
// ends.
func hold(path string) error {
	l, err := Listen(context.Background(), path)
	if err != nil {
		return err
	}
	defer l.Close()
	fmt.Println("listening")
	_, err = io.Copy(io.Discard, os.Stdin)
	return err
}

// roundTrip dials path and waits until l accepts the link that carries the
// frame it sent, closing links others dialed.
func roundTrip(l *Listener, path string) error {
	d, err := Dial(context.Background(), path)
	if err != nil {
		return err
	}
	defer d.Close()
	if err := d.Send(7, []byte("ping"), nil); err != nil {
		return err
	}
	deadline := time.Now().Add(5 * time.Second)
	l.SetDeadline(deadline)
	defer l.SetDeadline(time.Time{})
	for {
		a, err := l.Accept()
		if err != nil {
			return fmt.Errorf("Accept: %w", err)
		}
		a.SetReadDeadline(deadline)
		typ, p, _, err := a.Recv()
		a.Close()
		if err == nil && typ == 7 && string(p) == "ping" {
			return nil
		}
	}
}

// acceptOne fails the test unless l accepts a link dialed to path.
func acceptOne(t *testing.T, l *Listener, path string) {
	t.Helper()
	if err := roundTrip(l, path); err != nil {
		t.Fatal(err)
	}
}

func TestConcurrentListenOnAStalePathLeavesOneListener(t *testing.T) {
	const trials, starters = 200, 8
	var several, none, unreachable, unexpected int
	var firstErr error
	for range trials {
		path := sockPath(t)
		staleSocket(t, path)
		ls := make([]*Listener, starters)
		errs := make([]error, starters)
		var wg sync.WaitGroup
		start := make(chan struct{})
		for i := range starters {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				ls[i], errs[i] = Listen(t.Context(), path)
			}()
		}
		close(start)
		wg.Wait()
		var live []*Listener
		for i, err := range errs {
			switch {
			case err == nil:
				live = append(live, ls[i])
			case !isAddrInUse(err):
				unexpected++
				if firstErr == nil {
					firstErr = err
				}
			}
		}
		switch len(live) {
		case 0:
			none++
		case 1:
			if err := roundTrip(live[0], path); err != nil {
				unreachable++
			}
		default:
			several++
		}
		for _, l := range live {
			l.Close()
		}
	}
	if several+none+unreachable+unexpected > 0 {
		t.Fatalf("%d trials of %d concurrent Listen calls on a stale path: %d left several Listeners, %d none, %d one that accepts nothing; %d calls failed otherwise than EADDRINUSE, first %v",
			trials, starters, several, none, unreachable, unexpected, firstErr)
	}
}

func TestListenerCloseKeepsANewerSocketFileAtItsPath(t *testing.T) {
	path := sockPath(t)
	l := listen(t, path)
	if err := os.Remove(path); err != nil {
		t.Skipf("a bound socket file cannot be removed here: %v", err)
	}
	newer, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	defer newer.Close()
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(path); err != nil {
		t.Fatalf("Close removed the newer socket file: %v", err)
	}
	c, err := net.Dial("unix", path)
	if err != nil {
		t.Fatalf("newer listener unreachable: %v", err)
	}
	c.Close()
}

func TestListenDoesNotReplaceALiveServerWithoutTheLockFile(t *testing.T) {
	path := sockPath(t)
	live, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	defer live.Close()
	if l, err := Listen(t.Context(), path); err == nil {
		l.Close()
		t.Fatal("Listen replaced a live server")
	}
	c, err := net.Dial("unix", path)
	if err != nil {
		t.Fatalf("live server no longer reachable: %v", err)
	}
	c.Close()
}

func TestListenFailsWhileAnotherProcessHoldsThePathAndSucceedsOnceItIsKilled(t *testing.T) {
	path := sockPath(t)
	cmd := exec.Command(os.Args[0], "-test.run=^$")
	cmd.Env = append(os.Environ(), envHold+"="+path)
	cmd.Stderr = os.Stderr
	if _, err := cmd.StdinPipe(); err != nil {
		t.Fatal(err)
	}
	out, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cmd.Process.Kill()
		cmd.Wait()
	})
	if _, err := bufio.NewReader(out).ReadString('\n'); err != nil {
		t.Fatalf("holder did not listen: %v", err)
	}
	if l, err := Listen(t.Context(), path); !isAddrInUse(err) {
		if l != nil {
			l.Close()
		}
		t.Fatalf("Listen while another process holds the path: %v, want EADDRINUSE", err)
	}
	if err := cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	cmd.Wait()
	// Windows may take a moment to drop a dead process's lock.
	for deadline := time.Now().Add(10 * time.Second); ; time.Sleep(10 * time.Millisecond) {
		l, err := Listen(t.Context(), path)
		if err == nil {
			acceptOne(t, l, path)
			l.Close()
			return
		}
		if !isAddrInUse(err) || time.Now().After(deadline) {
			t.Fatalf("Listen after the holder was killed: %v", err)
		}
	}
}

func TestListenAndCloseInALoopFromEightGoroutinesNeverOverlap(t *testing.T) {
	const goroutines = 8
	path := sockPath(t)
	var live, served atomic.Int64
	end := time.Now().Add(2 * time.Second)
	errc := make(chan error, goroutines)
	for range goroutines {
		go func() {
			for time.Now().Before(end) {
				l, err := Listen(context.Background(), path)
				if isAddrInUse(err) {
					continue
				}
				if err != nil {
					errc <- fmt.Errorf("Listen: %w", err)
					return
				}
				n := live.Add(1)
				err = roundTrip(l, path)
				live.Add(-1)
				l.Close()
				if n > 1 {
					errc <- fmt.Errorf("%d Listeners live at once", n)
					return
				}
				if err != nil {
					errc <- err
					return
				}
				served.Add(1)
			}
			errc <- nil
		}()
	}
	for range goroutines {
		if err := <-errc; err != nil {
			t.Error(err)
		}
	}
	if n := served.Load(); n < 2 {
		t.Fatalf("%d Listeners served in 2 s; want them to take turns", n)
	}
	t.Logf("%d Listeners served in turn", served.Load())
}
