package main

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

// lingerFor is how long a stalled or lingering test peer runs before it
// exits by itself.
const lingerFor = 30 * time.Second

// misbehave runs a peer the way IPCBENCH_TEST_PEER says: "exit N" exits with
// status N before dialing, "stall" dials and then sends nothing, and
// "linger ADDR" runs as usual, then holds a connection to ADDR instead of
// exiting.
func misbehave(behavior string, args []string) int {
	verb, arg, _ := strings.Cut(behavior, " ")
	switch verb {
	case "exit":
		if code, err := strconv.Atoi(arg); err == nil {
			return code
		}
	case "stall":
		o, err := parseFlags(args, os.Stderr)
		if err != nil {
			return exitError
		}
		var conns []net.Conn
		defer func() { closeAll(conns) }() // kept reachable, so no finalizer closes them
		for range o.conns {
			c, err := dial(o)
			if err != nil {
				fmt.Fprintln(os.Stderr, "stall:", err)
				return exitError
			}
			conns = append(conns, c)
		}
		time.Sleep(lingerFor)
		return 0
	case "linger":
		code := run(args, os.Stdout, os.Stderr)
		c, err := net.Dial("tcp", arg)
		if err != nil {
			fmt.Fprintln(os.Stderr, "linger:", err)
			return exitError
		}
		defer c.Close()
		time.Sleep(lingerFor)
		return code
	}
	fmt.Fprintf(os.Stderr, "IPCBENCH_TEST_PEER %q: want exit N, stall or linger ADDR\n", behavior)
	return exitError
}

// setBound sets the bound *v to d until the test ends.
func setBound(t *testing.T, v *time.Duration, d time.Duration) {
	old := *v
	*v = d
	t.Cleanup(func() { *v = old })
}

// parsed is what parseFlags makes of args.
func parsed(t *testing.T, args ...string) options {
	t.Helper()
	o, err := parseFlags(args, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	return o
}

// A peer that exits before it connects fails the run at once rather than at
// the 30 s accept bound, with its Wait result in the message.
func TestPeerExitingBeforeConnectingFailsTheRunAtOnce(t *testing.T) {
	for _, tc := range []struct{ status, want string }{
		{"0", "ipcbench: peer exited before connecting: <nil>\n"},
		{"3", "ipcbench: peer exited before connecting: exit status 3\n"},
	} {
		t.Run("status "+tc.status, func(t *testing.T) {
			t.Setenv("IPCBENCH_TEST_PEER", "exit "+tc.status)
			start := time.Now()
			_, stderr, code := runBench(t, "-mode", "throughput", "-duration", "300ms")
			if d := time.Since(start); d > 10*time.Second {
				t.Fatalf("the run failed %v after it started, want within seconds", d)
			}
			if code != exitError || stderr != tc.want {
				t.Fatalf("exit code %d, stderr %q; want %d, %q", code, stderr, exitError, tc.want)
			}
		})
	}
}

// The first connection to fail ends the whole run at once, not at the end of
// its window, and its error is the one reported, not those of the
// connections closed in its wake.
func TestFirstFailingConnectionEndsTheRunAndIsReported(t *testing.T) {
	t.Setenv("IPCBENCH_AS_MAIN", "1")
	setBound(t, &peerGrace, time.Minute) // past the window, so killing the peer cannot end the run early
	var stderr bytes.Buffer
	start := time.Now()
	_, err := runParent(parsed(t, "-conns", "4", "-size", "4KiB", "-duration", "30s", "-corrupt", "5"), &stderr)
	if d := time.Since(start); d > 15*time.Second {
		t.Fatalf("the run ended %v into its 30s window, want at once; stderr:\n%s", d, &stderr)
	}
	var ie *integrityError
	if !errors.As(err, &ie) || ie.conn != 0 || !strings.Contains(err.Error(), "offset 16384") {
		t.Fatalf("runParent = %v, want connection 0's integrity failure; stderr:\n%s", err, &stderr)
	}
}

// A peer still running once the run has ended gets peerGrace to exit and is
// then killed: runParent returns with it gone, not when it would exit.
func TestPeerLingeringAfterTheRunIsKilledAfterTheGrace(t *testing.T) {
	held, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer held.Close()
	lingering := make(chan net.Conn, 1)
	go func() {
		if c, err := held.Accept(); err == nil {
			lingering <- c
		}
	}()
	t.Setenv("IPCBENCH_AS_MAIN", "1")
	t.Setenv("IPCBENCH_TEST_PEER", "linger "+held.Addr().String())
	setBound(t, &peerGrace, time.Second)
	var stderr bytes.Buffer
	start := time.Now()
	r, err := runParent(parsed(t, "-size", "4KiB", "-duration", "100ms"), &stderr)
	elapsed := time.Since(start)
	if err != nil || r.empty() {
		t.Fatalf("runParent = %v, want a measured run; stderr:\n%s", err, &stderr)
	}
	if elapsed < peerGrace || elapsed > lingerFor/2 {
		t.Fatalf("runParent returned after %v, want the peer killed once the %v grace had passed", elapsed, peerGrace)
	}
	var c net.Conn
	select {
	case c = <-lingering:
	case <-time.After(5 * time.Second):
		t.Fatalf("the peer did not linger; stderr:\n%s", &stderr)
	}
	defer c.Close()
	c.SetReadDeadline(time.Now().Add(5 * time.Second))
	if _, err := c.Read(make([]byte, 1)); errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatal("the peer still holds its connection after runParent returned")
	}
}

// A run closes its connections as it returns, which ends a latency peer's
// echo, so the peer exits at once rather than being killed after the grace.
func TestRunClosesItsConnectionsSoTheLatencyPeerExitsAtOnce(t *testing.T) {
	t.Setenv("IPCBENCH_AS_MAIN", "1")
	setBound(t, &peerGrace, 30*time.Second)
	var stderr bytes.Buffer
	start := time.Now()
	_, err := runParent(parsed(t, "-mode", "latency", "-duration", "100ms"), &stderr)
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("runParent = %v, want a latency run; stderr:\n%s", err, &stderr)
	}
	if elapsed > 10*time.Second {
		t.Fatalf("runParent returned after %v, want within seconds, not once the %v grace has killed the echoing peer; stderr:\n%s", elapsed, peerGrace, &stderr)
	}
}

// A run that outlasts -duration by safetyMargin has its connections closed
// and fails with the error that gave them, not with the bound's.
func TestRunPastItsSafetyBoundFailsWithTheConnectionError(t *testing.T) {
	t.Setenv("IPCBENCH_AS_MAIN", "1")
	t.Setenv("IPCBENCH_TEST_PEER", "stall")
	setBound(t, &safetyMargin, 300*time.Millisecond)
	setBound(t, &peerGrace, 300*time.Millisecond)
	var stderr bytes.Buffer
	start := time.Now()
	_, err := runParent(parsed(t, "-size", "4KiB", "-duration", "200ms"), &stderr)
	if d := time.Since(start); d > lingerFor/2 {
		t.Fatalf("runParent returned after %v, want soon after the 500ms bound", d)
	}
	if err == nil || !strings.HasPrefix(err.Error(), "connection 0: ") || !errors.Is(err, net.ErrClosed) {
		t.Fatalf("runParent = %v, want connection 0 failing with net.ErrClosed; stderr:\n%s", err, &stderr)
	}
}

// A peer that outlasts -duration by safetyMargin closes its connections and
// fails with their error.
func TestPeerPastItsSafetyBoundClosesItsConnections(t *testing.T) {
	setBound(t, &safetyMargin, 300*time.Millisecond)
	dir, err := os.MkdirTemp("", "ipc")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	path := filepath.Join(dir, "b.sock")
	o := parsed(t, "-mode", "latency", "-duration", "0s", "-path", path)
	ln, err := listen(o, path)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	errc := make(chan error, 1)
	go func() { errc <- runPeer(o) }()
	ln.SetDeadline(time.Now().Add(10 * time.Second))
	c, err := ln.Accept()
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	select {
	case err := <-errc:
		if !errors.Is(err, net.ErrClosed) {
			t.Fatalf("runPeer = %v, want net.ErrClosed from its own Close at the bound", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("runPeer still echoes 10s after its 300ms bound")
	}
}
