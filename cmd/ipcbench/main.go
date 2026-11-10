// Command ipcbench measures the throughput and latency of package ipc
// between two processes.
//
// The process you start listens and consumes; it re-executes itself with
// -peer to dial and produce, so every rate is measured on the consumer side
// after Release. Exit codes: 0 success, 1 error or nothing measured,
// 2 payload integrity failure.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/wzshiming/ipc"
)

const (
	exitError     = 1
	exitIntegrity = 2

	defaultRing = 4 << 20
	maxRing     = 1 << 30
	recHeader   = 8 // ring record header; MaxRecord = ring/2 - recHeader
)

type options struct {
	mode     string
	lanes    int
	size     int
	ring     int
	duration time.Duration
	spin     time.Duration
	json     bool
	path     string
	peer     bool
	corrupt  uint64
}

func main() { os.Exit(run(os.Args[1:], os.Stdout, os.Stderr)) }

func run(args []string, stdout, stderr io.Writer) int {
	o, err := parseFlags(args, stderr)
	if errors.Is(err, flag.ErrHelp) {
		return 0
	}
	if err != nil {
		fmt.Fprintln(stderr, "ipcbench:", err)
		return exitError
	}
	runtime.GOMAXPROCS(max(runtime.GOMAXPROCS(0), o.lanes+2))
	if o.peer {
		if err := runPeer(o); err != nil {
			fmt.Fprintln(stderr, "ipcbench peer:", err)
			return exitError
		}
		return 0
	}
	r, err := runParent(o, stderr)
	if err != nil {
		fmt.Fprintln(stderr, "ipcbench:", err)
		var ie *integrityError
		if errors.As(err, &ie) {
			return exitIntegrity
		}
		return exitError
	}
	if o.json {
		err = r.writeJSON(stdout)
	} else {
		err = r.writeText(stdout)
	}
	if err != nil {
		fmt.Fprintln(stderr, "ipcbench:", err)
		return exitError
	}
	if r.empty() {
		fmt.Fprintln(stderr, "ipcbench: no payload delivered; nothing was measured")
		return exitError
	}
	return 0
}

// sizeValue is a byte count flag accepting B, KiB, MiB and GiB suffixes.
type sizeValue int

func (s *sizeValue) String() string { return strconv.Itoa(int(*s)) }

func (s *sizeValue) Set(v string) error {
	n, err := parseSize(v)
	*s = sizeValue(n)
	return err
}

// parseSize parses a non-negative byte count with an optional B, KiB, MiB
// or GiB suffix.
func parseSize(s string) (int, error) {
	num, shift := s, 0
	for _, u := range []struct {
		suffix string
		shift  int
	}{{"KiB", 10}, {"MiB", 20}, {"GiB", 30}, {"B", 0}} {
		if strings.HasSuffix(s, u.suffix) {
			num, shift = strings.TrimSuffix(s, u.suffix), u.shift
			break
		}
	}
	n, err := strconv.ParseUint(num, 10, 63-shift)
	if err != nil {
		return 0, fmt.Errorf("invalid size %q: want bytes with an optional B, KiB, MiB or GiB suffix", s)
	}
	return int(n << shift), nil
}

func parseFlags(args []string, stderr io.Writer) (options, error) {
	var o options
	fs := flag.NewFlagSet("ipcbench", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.StringVar(&o.mode, "mode", "throughput", "throughput, framing or latency")
	fs.IntVar(&o.lanes, "lanes", 1, "number of lanes, each with its own producer and consumer thread")
	fs.Var((*sizeValue)(&o.size), "size", "record size in bytes, B/KiB/MiB/GiB suffixes allowed (default 1MiB for throughput, 64B otherwise)")
	fs.Var((*sizeValue)(&o.ring), "ring", "ring size per lane direction (default 4MiB, or the power of two holding 4 records)")
	fs.DurationVar(&o.duration, "duration", 5*time.Second, "how long the peer produces")
	fs.DurationVar(&o.spin, "spin", 0, "poll time before parking (0: library default, negative: park at once)")
	fs.BoolVar(&o.json, "json", false, "print the report as JSON")
	fs.StringVar(&o.path, "path", "", "control socket path (default: b.sock in a new temporary directory)")
	fs.BoolVar(&o.peer, "peer", false, "internal: run as the dialing, producing peer")
	fs.Uint64Var(&o.corrupt, "corrupt", 0, "internal, for tests: the peer corrupts record `N` of lane 0")
	if err := fs.Parse(args); err != nil {
		return o, err
	}
	if fs.NArg() > 0 {
		return o, fmt.Errorf("unexpected arguments %q", fs.Args())
	}
	return o, o.normalize()
}

// normalize fills mode-dependent defaults and rejects unusable settings.
func (o *options) normalize() error {
	switch o.mode {
	case "throughput":
		if o.size == 0 {
			o.size = 1 << 20
		}
	case "framing", "latency":
		if o.size == 0 {
			o.size = 64
		}
	default:
		return fmt.Errorf("-mode %q: want throughput, framing or latency", o.mode)
	}
	if o.mode == "latency" && o.lanes != 1 {
		return fmt.Errorf("-mode latency uses one lane, not -lanes %d", o.lanes)
	}
	if o.lanes < 1 {
		return fmt.Errorf("-lanes %d: want at least 1", o.lanes)
	}
	if o.size < 8 && o.mode != "framing" {
		return fmt.Errorf("-size %d: %s records carry an 8-byte sequence number", o.size, o.mode)
	}
	if o.size < 1 {
		return fmt.Errorf("-size %d: zero-length records mark the end of a lane", o.size)
	}
	if o.duration < 0 {
		return fmt.Errorf("-duration %v is negative", o.duration)
	}
	if o.ring == 0 {
		o.ring = defaultRing
		for o.ring < 4*(o.size+recHeader) && o.ring < maxRing {
			o.ring *= 2
		}
	}
	if limit := o.ring/2 - recHeader; o.size > limit {
		need := defaultRing
		for need/2-recHeader < o.size && need < maxRing {
			need *= 2
		}
		return fmt.Errorf("-size %d exceeds the largest record of a %d-byte ring (%d); use -ring %d or more", o.size, o.ring, limit, need)
	}
	return nil
}

// peerArgs is the command line of the peer that dials path.
func (o options) peerArgs(path string) []string {
	return []string{
		"-peer",
		"-mode=" + o.mode,
		"-lanes=" + strconv.Itoa(o.lanes),
		"-size=" + strconv.Itoa(o.size),
		"-ring=" + strconv.Itoa(o.ring),
		"-duration=" + o.duration.String(),
		"-spin=" + o.spin.String(),
		"-path=" + path,
		"-corrupt=" + strconv.FormatUint(o.corrupt, 10),
	}
}

// peer is the re-executed producing process.
type peer struct {
	cmd  *exec.Cmd
	done chan struct{}
	err  error
}

func startPeer(o options, path string, stderr io.Writer) (*peer, error) {
	exe, err := os.Executable()
	if err != nil {
		return nil, err
	}
	p := &peer{cmd: exec.Command(exe, o.peerArgs(path)...), done: make(chan struct{})}
	p.cmd.Stdout, p.cmd.Stderr = stderr, stderr
	if err := p.cmd.Start(); err != nil {
		return nil, err
	}
	go func() {
		p.err = p.cmd.Wait()
		close(p.done)
	}()
	return p, nil
}

// stop waits briefly for the peer to exit, then kills it.
func (p *peer) stop() {
	select {
	case <-p.done:
	case <-time.After(5 * time.Second):
		p.cmd.Process.Kill()
		<-p.done
	}
}

// runParent listens, starts the peer, consumes and reports.
func runParent(o options, stderr io.Writer) (*report, error) {
	path := o.path
	if path == "" {
		dir, err := os.MkdirTemp("", "ipc")
		if err != nil {
			return nil, err
		}
		defer os.RemoveAll(dir)
		path = filepath.Join(dir, "b.sock")
	}
	lc := ipc.ListenConfig{Lanes: o.lanes, RingSize: o.ring, Spin: o.spin}
	ln, err := lc.Listen(context.Background(), path)
	if err != nil {
		return nil, err
	}
	defer ln.Close()
	p, err := startPeer(o, path, stderr)
	if err != nil {
		return nil, err
	}
	defer p.stop()

	ln.SetDeadline(time.Now().Add(30 * time.Second))
	accepted := make(chan struct{})
	go func() {
		select {
		case <-p.done:
			ln.SetDeadline(time.Unix(1, 0))
		case <-accepted:
		}
	}()
	conn, err := ln.Accept()
	close(accepted)
	if err != nil {
		select {
		case <-p.done:
			return nil, fmt.Errorf("peer exited before connecting: %v", p.err)
		default:
			return nil, err
		}
	}
	defer conn.Close()
	if err := checkConn(o, conn); err != nil {
		return nil, err
	}
	stop := time.AfterFunc(o.duration+time.Minute, func() { conn.Close() })
	defer stop.Stop()
	r := newReport(o, runtime.GOMAXPROCS(0))
	if o.mode == "latency" {
		r.Latency, err = measureLatency(conn.Lane(0), o.size, o.duration)
	} else {
		err = r.consume(conn, o)
	}
	if err != nil {
		return nil, err
	}
	return r, nil
}

// checkConn rejects a connection granted other settings than asked.
func checkConn(o options, conn *ipc.Conn) error {
	if conn.Lanes() != o.lanes || conn.RingSize() != o.ring {
		return fmt.Errorf("granted %d lanes of %d-byte rings, asked %d of %d", conn.Lanes(), conn.RingSize(), o.lanes, o.ring)
	}
	if o.size > conn.MaxRecord() {
		return fmt.Errorf("-size %d exceeds MaxRecord %d", o.size, conn.MaxRecord())
	}
	return nil
}

// runPeer dials, then produces or echoes until the parent closes.
func runPeer(o options) error {
	d := ipc.Dialer{Lanes: o.lanes, RingSize: o.ring, Spin: o.spin, Timeout: time.Minute}
	conn, err := d.Dial(o.path)
	if err != nil {
		return err
	}
	defer conn.Close()
	if err := checkConn(o, conn); err != nil {
		return err
	}
	stop := time.AfterFunc(o.duration+time.Minute, func() { conn.Close() })
	defer stop.Stop()
	if o.mode == "latency" {
		err = echo(conn.Lane(0))
	} else {
		err = produceAll(conn, o)
	}
	if err != nil {
		select {
		case <-conn.Done():
			return nil // the parent quit first, e.g. on an integrity failure
		default:
			return err
		}
	}
	<-conn.Done()
	return nil
}
