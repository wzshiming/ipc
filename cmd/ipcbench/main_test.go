package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"os"
	"os/exec"
	"runtime"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// TestMain lets the tests run the real tool, and the peer it re-executes,
// from the test binary.
func TestMain(m *testing.M) {
	if os.Getenv("IPCBENCH_AS_MAIN") == "1" {
		peer := slices.ContainsFunc(os.Args[1:], func(a string) bool { return strings.HasPrefix(a, "-peer=") })
		if behavior := os.Getenv("IPCBENCH_TEST_PEER"); behavior != "" && peer {
			os.Exit(misbehave(behavior, os.Args[1:]))
		}
		os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
	}
	os.Exit(m.Run())
}

// runBench runs the tool as a separate process and returns its output and
// exit code.
func runBench(t *testing.T, args ...string) (stdout, stderr string, code int) {
	t.Helper()
	cmd := exec.Command(os.Args[0], args...)
	cmd.Env = append(os.Environ(), "IPCBENCH_AS_MAIN=1")
	var out, errOut bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &errOut
	err := cmd.Run()
	var exitErr *exec.ExitError
	switch {
	case err == nil:
	case errors.As(err, &exitErr):
		code = exitErr.ExitCode()
	default:
		t.Fatalf("run %v: %v", args, err)
	}
	return out.String(), errOut.String(), code
}

func TestParseSizeAcceptsSuffixesAndPlainBytes(t *testing.T) {
	for in, want := range map[string]int{
		"64KiB": 64 << 10,
		"1MiB":  1 << 20,
		"4MiB":  4 << 20,
		"1GiB":  1 << 30,
		"64B":   64,
		"4096":  4096,
		"0":     0,
	} {
		got, err := parseSize(in)
		if err != nil || got != want {
			t.Errorf("parseSize(%q) = %d, %v; want %d", in, got, err, want)
		}
	}
}

func TestParseSizeRejectsMalformedInput(t *testing.T) {
	for _, in := range []string{"", "KiB", "-1", "1.5MiB", "12XB", "1 MiB", "8GiB0", "99999999999GiB"} {
		if got, err := parseSize(in); err == nil {
			t.Errorf("parseSize(%q) = %d, want an error", in, got)
		}
	}
}

func TestFillStampsEachLineWithItsStreamOffset(t *testing.T) {
	p := make([]byte, 4*stride)
	fill(p, 2*stride)
	for line := 0; line < len(p); line += stride {
		if got, want := binary.LittleEndian.Uint64(p[line:]), uint64(2*stride+line); got != want {
			t.Fatalf("word at stream offset %d holds %d, want %d", want, got, want)
		}
	}
}

// What a chunk holds depends only on its stream offset, so chunks of any
// length and alignment make the same stream.
func TestFillIsAFunctionOfStreamOffset(t *testing.T) {
	whole := make([]byte, 4096)
	fill(whole, 0)
	part := make([]byte, 4096-1000)
	fill(part, 1000)
	if !bytes.Equal(part, whole[1000:]) {
		t.Fatal("fill(p, 1000) differs from bytes 1000.. of fill(p, 0)")
	}
}

func TestVerifyAcceptsChunksSplitInsideAStampWord(t *testing.T) {
	p := make([]byte, 2*stride)
	fill(p, 0)
	const cut = stride + 3
	if err := verify(p[:cut], 0); err != nil {
		t.Fatalf("first chunk: %v", err)
	}
	if err := verify(p[cut:], cut); err != nil {
		t.Fatalf("second chunk: %v", err)
	}
}

func TestVerifyRejectsFlippedByteOfASplitStampWord(t *testing.T) {
	p := make([]byte, 2*stride)
	fill(p, 0)
	const cut = stride + 3
	p[stride+5] ^= 0xff
	err := verify(p[cut:], cut)
	if err == nil || !strings.Contains(err.Error(), "offset 133") {
		t.Fatalf("verify of a split word flipped at offset 133 = %v", err)
	}
}

func TestVerifyRejectsFlippedByteOfAWholeStampWord(t *testing.T) {
	p := make([]byte, 4096)
	fill(p, 0)
	p[3*stride+2] ^= 0xff
	err := verify(p, 0)
	if err == nil || !strings.Contains(err.Error(), "offset 384") {
		t.Fatalf("verify of a word flipped at offset 386 = %v", err)
	}
}

func TestSourceFillsAtMostSizePerRead(t *testing.T) {
	const size = 64
	var stop atomic.Bool
	src := &source{size: size, payload: true, stop: &stop}
	p := make([]byte, 3*size)
	for off := uint64(0); off < 3*size; off += size {
		n, err := src.Read(p)
		if n != size || err != nil {
			t.Fatalf("Read of %d bytes = %d, %v; want %d, nil", len(p), n, err, size)
		}
		if err := verify(p[:n], off); err != nil {
			t.Fatalf("chunk at offset %d: %v", off, err)
		}
	}
	if n, err := src.Read(p[:size/2]); n != size/2 || err != nil {
		t.Fatalf("Read of %d bytes = %d, %v; want the whole slice, nil", size/2, n, err)
	}
}

func TestSourceWithoutPayloadBoundsReadsAndWritesNothing(t *testing.T) {
	var stop atomic.Bool
	src := &source{size: 64, stop: &stop}
	p := bytes.Repeat([]byte{0xaa}, 200)
	n, err := src.Read(p)
	if n != 64 || err != nil || !bytes.Equal(p, bytes.Repeat([]byte{0xaa}, 200)) {
		t.Fatalf("Read = %d, %v and touched the slice; want 64, nil and the slice as it was", n, err)
	}
}

func TestSourceReportsEOFOnceStopped(t *testing.T) {
	var stop atomic.Bool
	src := &source{size: 64, payload: true, stop: &stop}
	p := make([]byte, 64)
	if _, err := src.Read(p); err != nil {
		t.Fatal(err)
	}
	stop.Store(true)
	if n, err := src.Read(p); n != 0 || err != io.EOF {
		t.Fatalf("Read after stop = %d, %v; want 0, io.EOF", n, err)
	}
}

// The run ends by the producer's Close: the consumer stops at io.EOF with
// no end marker on the stream. Every Write after the first counts as one
// record, whatever its length.
func TestConsumeCountsEachChunkAfterTheFirstUntilProducerClose(t *testing.T) {
	chunks := []int{100, 300, 24, 600}
	producer, consumer := net.Pipe()
	consumer.SetDeadline(time.Now().Add(10 * time.Second))
	go func() {
		off := uint64(0)
		for _, n := range chunks {
			p := make([]byte, n)
			fill(p, off)
			producer.Write(p)
			off += uint64(n)
		}
		producer.Close()
	}()
	s, err := consume(consumer, 0, options{mode: "throughput"})
	if err != nil {
		t.Fatal(err)
	}
	if s.records != 3 || s.bytes != 300+24+600 || s.start.IsZero() || s.end.Before(s.start) {
		t.Fatalf("consumed %+v, want the 3 chunks after the first and a window from the first to EOF", s)
	}
}

func TestConsumeReportsIntegrityErrorWithTheOffset(t *testing.T) {
	producer, consumer := net.Pipe()
	consumer.SetDeadline(time.Now().Add(10 * time.Second))
	go func() {
		p := make([]byte, 1000)
		fill(p, 0)
		producer.Write(p)
		fill(p, 1000)
		p[1024-1000] ^= 0xff // the stamp word at stream offset 1024
		producer.Write(p)
		producer.Close()
	}()
	_, err := consume(consumer, 7, options{mode: "throughput"})
	var ie *integrityError
	if !errors.As(err, &ie) || !strings.Contains(err.Error(), "connection 7") || !strings.Contains(err.Error(), "offset 1024") {
		t.Fatalf("consume of a chunk flipped at offset 1024 = %v, want an integrity error naming connection 7 and offset 1024", err)
	}
}

// The producer ends its stream with Close, not a zero-length record: after
// stop the consumer reads whole records, then io.EOF.
func TestProduceEndsWithClose(t *testing.T) {
	const size = 64
	producer, consumer := net.Pipe()
	consumer.SetDeadline(time.Now().Add(10 * time.Second))
	var stop atomic.Bool
	errc := make(chan error, 1)
	go func() {
		errc <- produce(producer, 0, options{size: size}, &source{size: size, payload: true, stop: &stop})
	}()
	rec := make([]byte, size)
	for range 3 {
		if _, err := io.ReadFull(consumer, rec); err != nil {
			t.Fatal(err)
		}
	}
	stop.Store(true)
	rest, err := io.ReadAll(consumer)
	if err != nil || len(rest)%size != 0 {
		t.Fatalf("after stop the stream held %d bytes, %v; want whole %d-byte records then io.EOF", len(rest), err, size)
	}
	if err := <-errc; err != nil {
		t.Fatal(err)
	}
}

// The parent's Close ends the echo loop: echo returns nil at io.EOF.
func TestEchoEndsAtPeerClose(t *testing.T) {
	const size = 64
	parent, peer := net.Pipe()
	parent.SetDeadline(time.Now().Add(10 * time.Second))
	peer.SetDeadline(time.Now().Add(10 * time.Second))
	errc := make(chan error, 1)
	go func() { errc <- echo(peer, size) }()
	msg := newTemplate(size)
	if _, err := parent.Write(msg); err != nil {
		t.Fatal(err)
	}
	reply := make([]byte, size)
	if _, err := io.ReadFull(parent, reply); err != nil || !bytes.Equal(reply, msg) {
		t.Fatalf("echo replied %x, %v; want the record sent", reply, err)
	}
	parent.Close()
	if err := <-errc; err != nil {
		t.Fatalf("echo after the parent's Close = %v, want nil", err)
	}
}

// Chunk 5 of 4 KiB starts at stream offset 16384, whose stamp word the
// flipped first byte damages.
func TestCorruptChunkExitsWithCodeTwo(t *testing.T) {
	t.Setenv("IPCBENCH_TEST_PEER", "corrupt 5")
	_, stderr, code := runBench(t, "-mode", "throughput", "-size", "4KiB", "-duration", "300ms")
	if code != 2 {
		t.Fatalf("exit code %d, want 2; stderr:\n%s", code, stderr)
	}
	if !strings.Contains(stderr, "connection 0") || !strings.Contains(stderr, "offset 16384") {
		t.Fatalf("stderr does not name connection 0 and offset 16384:\n%s", stderr)
	}
}

func TestRunReportsItsConfigurationAndMeasurements(t *testing.T) {
	t.Setenv("IPCBENCH_AS_MAIN", "1")
	kernelThroughput := func(network string) func(r *report) bool {
		return func(r *report) bool {
			return r.Net == network && r.Ring == 0 && r.Aggregate != nil && r.Aggregate.GBps > 0
		}
	}
	roundTrips := func(r *report) bool { return r.Latency != nil && r.Latency.Samples > 0 && r.Latency.P50 > 0 }
	for _, tc := range []struct {
		args []string
		want string
		ok   func(r *report) bool
	}{
		{[]string{"-mode", "throughput", "-conns", "2", "-size", "64KiB"}, "throughput of 2 connections of 64 KiB records, each and all at positive rates", func(r *report) bool {
			a := r.Aggregate
			return r.Mode == "throughput" && r.Conns == 2 && r.Size == 64<<10 && len(r.PerConn) == 2 &&
				a != nil && a.GBps > 0 && a.Gbps > 0 && a.RecordsPerSec > 0 &&
				!slices.ContainsFunc(r.PerConn, func(c rate) bool { return c.GBps <= 0 })
		}},
		{[]string{"-mode", "framing", "-size", "64B"}, "positive records/s", func(r *report) bool {
			return r.Aggregate != nil && r.Aggregate.RecordsPerSec > 0
		}},
		{[]string{"-mode", "latency"}, "positive percentiles in order", func(r *report) bool {
			l := r.Latency
			return l != nil && l.Samples > 0 && l.P50 > 0 && l.P99 >= l.P50 && l.P999 >= l.P99
		}},
		{[]string{"-mode", "throughput", "-net", "unix", "-size", "64KiB"}, "net unix, ring 0 and a positive rate", kernelThroughput("unix")},
		{[]string{"-mode", "throughput", "-net", "tcp", "-size", "64KiB"}, "net tcp, ring 0 and a positive rate", kernelThroughput("tcp")},
		{[]string{"-mode", "latency", "-net", "unix"}, "measured round trips", roundTrips},
		{[]string{"-mode", "latency", "-net", "tcp"}, "measured round trips", roundTrips},
		{[]string{"-mode", "throughput", "-io", "rw", "-size", "64KiB"}, "io rw, positive rates and 64 KiB per record", func(r *report) bool {
			a := r.Aggregate
			return r.IO == "rw" && a != nil && a.GBps > 0 && a.RecordsPerSec > 0 && a.Bytes == a.Records*(64<<10)
		}},
	} {
		t.Run(strings.Join(tc.args, " "), func(t *testing.T) {
			var stderr bytes.Buffer
			r, err := runParent(parsed(t, append(tc.args, "-duration", "300ms")...), &stderr)
			if err != nil || r.empty() {
				t.Fatalf("runParent = %v, want a measured run; stderr:\n%s", err, &stderr)
			}
			if !tc.ok(r) {
				t.Fatalf("report %+v with aggregate %+v and latency %+v, want %s", *r, r.Aggregate, r.Latency, tc.want)
			}
		})
	}
}

func TestThroughputTextReportsRates(t *testing.T) {
	stdout, stderr, code := runBench(t, "-mode", "throughput", "-conns", "2", "-size", "64KiB", "-duration", "300ms")
	if code != 0 {
		t.Fatalf("exit code %d; stderr:\n%s", code, stderr)
	}
	for _, want := range []string{"conns=2", "conn", "GB/s", "Gbps", "records/s", "all"} {
		if !strings.Contains(stdout, want) {
			t.Fatalf("text report lacks %q:\n%s", want, stdout)
		}
	}
}

func TestRemovedFlagIsUnknown(t *testing.T) {
	for _, args := range [][]string{{"-lanes", "2"}, {"-corrupt", "5"}, {"-path", "b.sock"}, {"-spin", "1ms"}, {"-json"}} {
		_, err := parseFlags(args, &bytes.Buffer{})
		if err == nil || !strings.Contains(err.Error(), "not defined") {
			t.Errorf("parseFlags(%s) = %v, want a flag-not-defined error", strings.Join(args, " "), err)
		}
	}
}

func TestLatencyModeRequiresOneConnection(t *testing.T) {
	if _, err := parseFlags([]string{"-mode", "latency", "-conns", "2"}, &bytes.Buffer{}); err == nil {
		t.Fatal("-mode latency -conns 2 was accepted")
	}
	if _, err := parseFlags([]string{"-mode", "latency", "-conns", "1"}, &bytes.Buffer{}); err != nil {
		t.Fatalf("-mode latency -conns 1 was rejected: %v", err)
	}
}

func TestZeroThroughputExitsNonZero(t *testing.T) {
	_, stderr, code := runBench(t, "-mode", "throughput", "-size", "4KiB", "-duration", "0s")
	if code == 0 {
		t.Fatal("a run that delivered no payload exited 0")
	}
	if !strings.Contains(stderr, "no payload") {
		t.Fatalf("stderr does not explain the failure:\n%s", stderr)
	}
}

func TestSizeAboveRingMaxRecordIsRejected(t *testing.T) {
	_, stderr, code := runBench(t, "-size", "1MiB", "-ring", "1MiB", "-duration", "100ms")
	if code != 1 || !strings.Contains(stderr, "-ring") {
		t.Fatalf("exit code %d, want 1 with a -ring hint; stderr:\n%s", code, stderr)
	}
}

func TestDefaultRingFitsLargeRecords(t *testing.T) {
	o, err := parseFlags([]string{"-size", "1MiB"}, &bytes.Buffer{})
	if err != nil {
		t.Fatal(err)
	}
	if o.size > o.ring/2-8 {
		t.Fatalf("default ring %d cannot carry %d-byte records", o.ring, o.size)
	}
}

func TestUnknownTransportAndIOAreRejected(t *testing.T) {
	_, err := parseFlags([]string{"-net", "udp"}, &bytes.Buffer{})
	if err == nil || !strings.Contains(err.Error(), "want ipc, unix or tcp") {
		t.Errorf("-net udp = %v, want an error listing the transports", err)
	}
	_, err = parseFlags([]string{"-io", "splice"}, &bytes.Buffer{})
	if err == nil || !strings.Contains(err.Error(), "want copy or rw") {
		t.Errorf("-io splice = %v, want an error listing the I/O paths", err)
	}
}

// Kernel sockets have no ring: -ring is not applied, no size check against
// it runs, and the report shows ring 0.
func TestKernelTransportsHaveNoRing(t *testing.T) {
	for _, network := range []string{"unix", "tcp"} {
		o, err := parseFlags([]string{"-net", network, "-size", "1MiB", "-ring", "1MiB"}, &bytes.Buffer{})
		if err != nil {
			t.Fatalf("-net %s -size 1MiB -ring 1MiB was rejected: %v", network, err)
		}
		if o.ring != 0 {
			t.Fatalf("-net %s normalized to ring %d, want 0", network, o.ring)
		}
	}
}

func TestCorruptChunkOnKernelTransportsExitsWithCodeTwo(t *testing.T) {
	t.Setenv("IPCBENCH_TEST_PEER", "corrupt 5")
	for _, network := range []string{"unix", "tcp"} {
		_, stderr, code := runBench(t, "-mode", "throughput", "-net", network, "-size", "4KiB", "-duration", "300ms")
		if code != 2 || !strings.Contains(stderr, "connection 0") || !strings.Contains(stderr, "offset 16384") {
			t.Fatalf("-net %s: exit code %d, want 2 naming connection 0 and offset 16384; stderr:\n%s", network, code, stderr)
		}
	}
}

// Once the parent's run bound closes a kernel socket, the peer's next Write
// fails with the socket's reset; that ends the peer's run and is not an error.
func TestPeerExitsQuietlyWhenParentClosesKernelTransportFirst(t *testing.T) {
	t.Setenv("IPCBENCH_AS_MAIN", "1")
	setBound(t, &safetyMargin, -29*time.Second) // the parent's bound closes 1s into the peer's 30s
	setBound(t, &peerGrace, 30*time.Second)
	networks := []string{"unix", "tcp"}
	if runtime.GOOS == "darwin" {
		// darwin may fail the peer's unix Write with ENOTCONN, which peerGone does not list.
		networks = []string{"tcp"}
	}
	for _, network := range networks {
		var stderr bytes.Buffer
		start := time.Now()
		_, err := runParent(parsed(t, "-net", network, "-size", "4KiB", "-duration", "30s"), &stderr)
		if !errors.Is(err, net.ErrClosed) {
			t.Fatalf("-net %s: runParent = %v, want its bound to close the connections first; stderr:\n%s", network, err, &stderr)
		}
		if d := time.Since(start); d > 15*time.Second {
			t.Fatalf("-net %s: runParent returned after %v, want the peer gone by itself, not killed after the %v grace", network, d, peerGrace)
		}
		if strings.Contains(stderr.String(), "ipcbench peer:") {
			t.Fatalf("-net %s: the peer reported an error after the parent closed:\n%s", network, &stderr)
		}
	}
}

func TestReportNamesTransportAndIO(t *testing.T) {
	r := newReport(options{mode: "throughput", net: "tcp", io: "rw", conns: 1, size: 64}, 4)
	var text bytes.Buffer
	if err := r.writeText(&text); err != nil {
		t.Fatal(err)
	}
	if want := "mode=throughput net=tcp io=rw conns=1 size=64 ring=0 duration=0s gomaxprocs=4\n"; !strings.HasPrefix(text.String(), want) {
		t.Fatalf("text header is not %q:\n%s", want, text.String())
	}
}

// A -io rw stream ends with Close after whole messages; a short last one
// is a truncated stream, an integrity failure naming the message.
func TestConsumeWithReadWriteIORejectsAShortLastMessage(t *testing.T) {
	const size = 256
	producer, consumer := net.Pipe()
	consumer.SetDeadline(time.Now().Add(10 * time.Second))
	go func() {
		p := make([]byte, size+size/2)
		fill(p, 0)
		producer.Write(p)
		producer.Close()
	}()
	_, err := consume(consumer, 3, options{mode: "throughput", io: "rw", size: size})
	var ie *integrityError
	if !errors.As(err, &ie) || !strings.Contains(err.Error(), "connection 3") || !strings.Contains(err.Error(), "message 2") {
		t.Fatalf("consume of 1.5 messages = %v, want an integrity error naming connection 3 and message 2", err)
	}
}

// With -io rw the peer flips the first byte of message 5, stream offset 16384.
func TestCorruptMessageWithReadWriteIOExitsWithCodeTwo(t *testing.T) {
	t.Setenv("IPCBENCH_TEST_PEER", "corrupt 5")
	_, stderr, code := runBench(t, "-mode", "throughput", "-io", "rw", "-size", "4KiB", "-duration", "300ms")
	if code != 2 {
		t.Fatalf("exit code %d, want 2; stderr:\n%s", code, stderr)
	}
	if !strings.Contains(stderr, "connection 0") || !strings.Contains(stderr, "offset 16384") {
		t.Fatalf("stderr does not name connection 0 and offset 16384:\n%s", stderr)
	}
}

func TestCompareRejectsSingleConfigurationFlags(t *testing.T) {
	for _, args := range [][]string{{"-conns", "1"}, {"-size", "64KiB"}, {"-net", "ipc"}, {"-io", "copy"}} {
		_, err := parseFlags(append([]string{"-mode", "compare"}, args...), &bytes.Buffer{})
		if err == nil || !strings.Contains(err.Error(), "compare") || !strings.Contains(err.Error(), args[0]) {
			t.Errorf("-mode compare %s = %v, want an error naming %s and compare", strings.Join(args, " "), err, args[0])
		}
	}
	if _, err := parseFlags([]string{"-mode", "compare", "-duration", "1s"}, &bytes.Buffer{}); err != nil {
		t.Fatalf("-mode compare -duration 1s was rejected: %v", err)
	}
}

// compareRows returns the body rows of every markdown table in out.
func compareRows(out string) []string {
	var rows []string
	for _, line := range strings.Split(out, "\n") {
		if strings.HasPrefix(line, "| ") && !strings.HasPrefix(line, "| Transport") && !strings.HasPrefix(line, "| ---") {
			rows = append(rows, line)
		}
	}
	return rows
}

func TestCompareListsEveryConfigurationInOrder(t *testing.T) {
	stdout, stderr, code := runBench(t, "-mode", "compare", "-duration", "150ms")
	if code != 0 {
		t.Fatalf("exit code %d; stderr:\n%s", code, stderr)
	}
	if first := strings.SplitN(stdout, "\n", 2)[0]; !strings.Contains(first, runtime.GOOS+"/"+runtime.GOARCH) || !strings.Contains(first, "150ms") {
		t.Fatalf("first line %q does not name the platform and duration", first)
	}
	for _, header := range []string{
		"| Transport | I/O | Connections | Record size | GB/s | Gbps | GB/s per connection |",
		"| Transport | p50 | p99 | p999 |",
	} {
		if !strings.Contains(stdout, header) {
			t.Fatalf("output lacks the table header %q:\n%s", header, stdout)
		}
	}
	want := []string{
		"| ipc | copy | 1 | 64 KiB |",
		"| unix | copy | 1 | 64 KiB |",
		"| tcp | copy | 1 | 64 KiB |",
		"| ipc | copy | 2 | 64 KiB |",
		"| ipc | copy | 4 | 64 KiB |",
		"| ipc | copy | 1 | 4 KiB |",
		"| ipc | copy | 1 | 1 MiB |",
		"| ipc | rw | 1 | 64 KiB |",
		"| ipc |",
		"| unix |",
		"| tcp |",
	}
	rows := compareRows(stdout)
	if len(rows) != len(want) {
		t.Fatalf("%d table rows, want %d:\n%s", len(rows), len(want), stdout)
	}
	for i, w := range want {
		if !strings.HasPrefix(rows[i], w) {
			t.Fatalf("row %d is %q, want it to start with %q", i, rows[i], w)
		}
	}
}
