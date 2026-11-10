package main

import (
	"errors"
	"fmt"
	"io"
	"runtime"
	"strings"
)

// comparison is one throughput configuration of -mode compare.
type comparison struct {
	net, io     string
	conns, size int
}

// compareThroughput is the throughput table's rows, in order.
var compareThroughput = []comparison{
	{"ipc", "copy", 1, 64 << 10},
	{"unix", "copy", 1, 64 << 10},
	{"tcp", "copy", 1, 64 << 10},
	{"ipc", "copy", 2, 64 << 10},
	{"ipc", "copy", 4, 64 << 10},
	{"ipc", "copy", 1, 4 << 10},
	{"ipc", "copy", 1, 1 << 20},
	{"ipc", "rw", 1, 64 << 10},
}

// compareLatency is the transports of the 64 B round-trip table's rows.
var compareLatency = []string{"ipc", "unix", "tcp"}

// runCompare measures every configuration of the set, each a full
// two-process run of o.duration, and writes markdown tables of the results
// to stdout. procs is GOMAXPROCS before any run, so each configuration gets
// what a single run of it would.
func runCompare(o options, procs int, stdout, stderr io.Writer) error {
	measure := func(c options) (*report, error) {
		if err := c.normalize(); err != nil {
			return nil, err
		}
		runtime.GOMAXPROCS(max(procs, c.conns+2))
		r, err := runParent(c, stderr)
		if err == nil && r.empty() {
			err = errors.New("no payload delivered; nothing was measured")
		}
		if err != nil {
			return nil, fmt.Errorf("-mode %s -net %s -io %s -conns %d -size %d: %w", c.mode, c.net, c.io, c.conns, c.size, err)
		}
		return r, nil
	}
	var b strings.Builder
	fmt.Fprintf(&b, "ipcbench -mode compare on %s/%s, %d CPUs, %v per configuration\n", runtime.GOOS, runtime.GOARCH, runtime.NumCPU(), o.duration)
	b.WriteString("\n| Transport | I/O | Connections | Record size | GB/s | Gbps | GB/s per connection |\n")
	b.WriteString("| --- | --- | ---: | ---: | ---: | ---: | ---: |\n")
	for _, x := range compareThroughput {
		c := o
		c.mode, c.net, c.io, c.conns, c.size = "throughput", x.net, x.io, x.conns, x.size
		r, err := measure(c)
		if err != nil {
			return err
		}
		fmt.Fprintf(&b, "| %s | %s | %d | %s | %.2f | %.1f | %.2f |\n",
			x.net, x.io, x.conns, formatSize(x.size), r.Aggregate.GBps, r.Aggregate.Gbps, r.Aggregate.GBps/float64(x.conns))
	}
	b.WriteString("\n64 B round trip:\n\n| Transport | p50 | p99 | p999 |\n")
	b.WriteString("| --- | ---: | ---: | ---: |\n")
	for _, network := range compareLatency {
		c := o
		c.mode, c.net, c.size = "latency", network, 64
		r, err := measure(c)
		if err != nil {
			return err
		}
		fmt.Fprintf(&b, "| %s | %.2f µs | %.2f µs | %.2f µs |\n", network, r.Latency.P50, r.Latency.P99, r.Latency.P999)
	}
	b.WriteString("\nio.Copy delivers ipc's records in place; on unix and tcp it copies through its own 32 KiB buffer, so their chunks are at most 32 KiB.\n")
	_, err := io.WriteString(stdout, b.String())
	return err
}

// formatSize writes n as whole MiB, KiB or B.
func formatSize(n int) string {
	switch {
	case n >= 1<<20 && n%(1<<20) == 0:
		return fmt.Sprintf("%d MiB", n>>20)
	case n >= 1<<10 && n%(1<<10) == 0:
		return fmt.Sprintf("%d KiB", n>>10)
	}
	return fmt.Sprintf("%d B", n)
}
