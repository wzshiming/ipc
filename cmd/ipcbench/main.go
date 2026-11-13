// Command ipcbench measures the throughput and latency of package ipc
// between two processes, and of unix and tcp sockets for comparison.
//
// The process you start listens and consumes; it re-executes itself with
// -peer to dial and produce, so every rate is measured on the consumer side
// as each record is delivered. -mode compare runs a fixed set of
// configurations and prints markdown tables. Exit codes: 0 success, 1 error
// or nothing measured, 2 payload integrity failure.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
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
	net      string
	io       string
	conns    int
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
	procs := runtime.GOMAXPROCS(0)
	if o.mode == "compare" {
		if err := runCompare(o, procs, stdout, stderr); err != nil {
			fmt.Fprintln(stderr, "ipcbench:", err)
			return exitError
		}
		return 0
	}
	runtime.GOMAXPROCS(max(procs, o.conns+2))
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
	fs.StringVar(&o.mode, "mode", "throughput", "throughput, framing, latency or compare")
	fs.StringVar(&o.net, "net", "ipc", "transport under test: ipc, unix or tcp")
	fs.StringVar(&o.io, "io", "copy", "throughput and framing I/O: copy (io.Copy on both ends) or rw (one Write and io.ReadFull per record)")
	fs.IntVar(&o.conns, "conns", 1, "number of connections, each with its own producer and consumer thread")
	fs.Var((*sizeValue)(&o.size), "size", "record size: what the producer offers per Read, or writes per message with -io rw; B/KiB/MiB/GiB suffixes allowed (default 1MiB for throughput, 64B otherwise)")
	fs.Var((*sizeValue)(&o.ring), "ring", "ring size per direction of -net ipc (default 4MiB, or the power of two holding 4 records)")
	fs.DurationVar(&o.duration, "duration", 5*time.Second, "how long the peer produces")
	fs.DurationVar(&o.spin, "spin", 0, "poll time before parking of -net ipc (0: library default, negative: park at once)")
	fs.BoolVar(&o.json, "json", false, "print the report as JSON")
	fs.StringVar(&o.path, "path", "", "socket path of -net ipc and unix (default: b.sock in a new temporary directory)")
	fs.BoolVar(&o.peer, "peer", false, "internal: run as the dialing, producing peer")
	fs.Uint64Var(&o.corrupt, "corrupt", 0, "internal, for tests: the peer flips the first byte of chunk `N` of connection 0")
	if err := fs.Parse(args); err != nil {
		return o, err
	}
	if fs.NArg() > 0 {
		return o, fmt.Errorf("unexpected arguments %q", fs.Args())
	}
	if o.mode == "compare" {
		var set []string
		fs.Visit(func(f *flag.Flag) {
			if slices.Contains([]string{"conns", "size", "net", "io", "json"}, f.Name) {
				set = append(set, "-"+f.Name)
			}
		})
		if len(set) > 0 {
			return o, fmt.Errorf("%s: -mode compare runs a fixed set of configurations", strings.Join(set, ", "))
		}
	}
	return o, o.normalize()
}

// normalize fills mode-dependent defaults and rejects unusable settings.
func (o *options) normalize() error {
	switch o.net {
	case "ipc", "unix", "tcp":
	default:
		return fmt.Errorf("-net %q: want ipc, unix or tcp", o.net)
	}
	switch o.io {
	case "copy", "rw":
	default:
		return fmt.Errorf("-io %q: want copy or rw", o.io)
	}
	if o.duration < 0 {
		return fmt.Errorf("-duration %v is negative", o.duration)
	}
	switch o.mode {
	case "throughput":
		if o.size == 0 {
			o.size = 1 << 20
		}
	case "framing", "latency":
		if o.size == 0 {
			o.size = 64
		}
	case "compare":
		return nil // each configuration of the set is normalized as it runs
	default:
		return fmt.Errorf("-mode %q: want throughput, framing, latency or compare", o.mode)
	}
	if o.mode == "latency" && o.conns != 1 {
		return fmt.Errorf("-mode latency uses one connection, not -conns %d", o.conns)
	}
	if o.conns < 1 {
		return fmt.Errorf("-conns %d: want at least 1", o.conns)
	}
	if o.size < 8 && o.mode != "framing" {
		return fmt.Errorf("-size %d: %s records carry an 8-byte sequence number", o.size, o.mode)
	}
	if o.net != "ipc" {
		o.ring = 0 // kernel sockets have no ring
		return nil
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

// peerArgs is the command line of the peer that dials addr.
func (o options) peerArgs(addr string) []string {
	return []string{
		"-peer",
		"-mode=" + o.mode,
		"-net=" + o.net,
		"-io=" + o.io,
		"-conns=" + strconv.Itoa(o.conns),
		"-size=" + strconv.Itoa(o.size),
		"-ring=" + strconv.Itoa(o.ring),
		"-duration=" + o.duration.String(),
		"-spin=" + o.spin.String(),
		"-path=" + addr,
		"-corrupt=" + strconv.FormatUint(o.corrupt, 10),
	}
}

// Bounds of a run's end; tests set their own.
var (
	safetyMargin = time.Minute     // past -duration, a run's connections are closed
	peerGrace    = 5 * time.Second // after a run, the peer's time to exit before it is killed
)

// startPeer starts the peer, which has peerGrace to exit once ctx is done
// before it is killed. The context returned is done once the peer has been
// reaped, with Wait's error as the cause.
func startPeer(ctx context.Context, o options, addr string, stderr io.Writer) (context.Context, error) {
	exe, err := os.Executable()
	if err != nil {
		return nil, err
	}
	cmd := exec.CommandContext(ctx, exe, o.peerArgs(addr)...)
	cmd.Cancel = nil // the run's end leaves the peer to exit by itself
	cmd.WaitDelay = peerGrace
	cmd.Stdout, cmd.Stderr = stderr, stderr
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	exited, exit := context.WithCancelCause(context.Background())
	go func() { exit(cmd.Wait()) }()
	return exited, nil
}

// waitErr is the peer's Wait error, exited's cause, which records nil as context.Canceled.
func waitErr(exited context.Context) error {
	if err := context.Cause(exited); err != context.Canceled {
		return err
	}
	return nil
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
	ln, err := listen(o, path)
	if err != nil {
		return nil, err
	}
	defer ln.Close()
	ctx, end := context.WithCancelCause(context.Background())
	exited, err := startPeer(ctx, o, ln.Addr().String(), stderr)
	if err != nil {
		end(nil)
		return nil, err
	}
	defer func() {
		end(nil)
		<-exited.Done()
	}()
	conns, err := accept(ln, o.conns, exited)
	if err != nil {
		return nil, err
	}
	defer closeAll(conns)
	bounded, stop := context.WithTimeout(ctx, o.duration+safetyMargin)
	defer stop()
	stopClosing := context.AfterFunc(bounded, func() { closeAll(conns) })
	defer stopClosing()
	r := newReport(o, runtime.GOMAXPROCS(0))
	if o.mode == "latency" {
		r.Latency, err = measureLatency(conns[0], o.size, o.duration)
	} else {
		err = r.consume(ctx, end, conns, o)
	}
	if err != nil {
		return nil, err
	}
	return r, nil
}

// listener is a net.Listener whose Accept a deadline can give up, as every
// -net transport's is.
type listener interface {
	net.Listener
	SetDeadline(time.Time) error
}

// listen opens the parent's -net listener; ipc and unix sockets live at path.
func listen(o options, path string) (listener, error) {
	switch o.net {
	case "unix":
		return net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
	case "tcp":
		return net.ListenTCP("tcp", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	}
	lc := ipc.ListenConfig{RingSize: o.ring, Spin: o.spin}
	return lc.Listen(context.Background(), path)
}

// accept takes n connections from ln, giving up once the peer has exited.
func accept(ln listener, n int, exited context.Context) ([]net.Conn, error) {
	ln.SetDeadline(time.Now().Add(30 * time.Second))
	stop := context.AfterFunc(exited, func() { ln.SetDeadline(time.Unix(1, 0)) })
	defer stop()
	conns := make([]net.Conn, 0, n)
	for len(conns) < n {
		c, err := ln.Accept()
		if err != nil {
			closeAll(conns)
			select {
			case <-exited.Done():
				return nil, fmt.Errorf("peer exited before connecting: %v", waitErr(exited))
			default:
				return nil, err
			}
		}
		conns = append(conns, c)
	}
	return conns, nil
}

func closeAll(conns []net.Conn) {
	for _, c := range conns {
		c.Close()
	}
}

// dial connects the peer to the parent's listener at o.path over -net.
func dial(o options) (net.Conn, error) {
	switch o.net {
	case "unix", "tcp":
		return net.Dial(o.net, o.path)
	}
	d := ipc.Dialer{RingSize: o.ring, Spin: o.spin, Timeout: time.Minute}
	return d.Dial(o.path)
}

// runPeer dials, then produces or echoes until the run ends.
func runPeer(o options) error {
	conns := make([]net.Conn, 0, o.conns)
	defer func() { closeAll(conns) }()
	for len(conns) < o.conns {
		c, err := dial(o)
		if err != nil {
			return err
		}
		conns = append(conns, c)
	}
	ctx, stop := context.WithTimeout(context.Background(), o.duration+safetyMargin)
	defer stop()
	stopClosing := context.AfterFunc(ctx, func() { closeAll(conns) })
	defer stopClosing()
	var err error
	if o.mode == "latency" {
		err = echo(conns[0], o.size)
	} else {
		err = produceAll(conns, o)
	}
	if errors.Is(err, ipc.ErrPeerClosed) || peerGone(err) {
		return nil // the parent quit first, e.g. on an integrity failure, and reports why
	}
	return err
}
