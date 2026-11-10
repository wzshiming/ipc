package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"strings"
	"testing"
)

// TestMain lets the tests run the real tool, and the peer it re-executes,
// from the test binary.
func TestMain(m *testing.M) {
	if os.Getenv("IPCBENCH_AS_MAIN") == "1" {
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

func TestVerifyRecordAcceptsStampedTemplate(t *testing.T) {
	tmpl := newTemplate(4096 + 100)
	stampTemplate(tmpl, 7)
	if err := verifyRecord(tmpl, 7); err != nil {
		t.Fatal(err)
	}
}

func TestVerifyRecordRejectsWrongSequence(t *testing.T) {
	tmpl := newTemplate(4096)
	stampTemplate(tmpl, 7)
	if err := verifyRecord(tmpl, 8); err == nil {
		t.Fatal("record stamped 7 verified as 8")
	}
}

func TestVerifyRecordRejectsCorruptedTemplate(t *testing.T) {
	tmpl := newTemplate(4096)
	stampTemplate(tmpl, 7)
	tmpl[3*128+2] ^= 0xff
	err := verifyRecord(tmpl, 7)
	if err == nil || !strings.Contains(err.Error(), "offset 384") {
		t.Fatalf("verifyRecord on corrupted word at 384 = %v", err)
	}
}

func TestCorruptRecordExitsWithCodeTwo(t *testing.T) {
	_, stderr, code := runBench(t, "-mode", "throughput", "-size", "4KiB", "-duration", "300ms", "-corrupt", "5")
	if code != 2 {
		t.Fatalf("exit code %d, want 2; stderr:\n%s", code, stderr)
	}
	if !strings.Contains(stderr, "connection 0 record 5") {
		t.Fatalf("stderr does not name connection 0 record 5:\n%s", stderr)
	}
}

func TestThroughputJSONParsesWithPositiveRates(t *testing.T) {
	stdout, stderr, code := runBench(t, "-mode", "throughput", "-conns", "2", "-size", "64KiB", "-duration", "300ms", "-json")
	if code != 0 {
		t.Fatalf("exit code %d; stderr:\n%s", code, stderr)
	}
	var r report
	if err := json.Unmarshal([]byte(stdout), &r); err != nil {
		t.Fatalf("output is not JSON: %v\n%s", err, stdout)
	}
	if r.Mode != "throughput" || r.Conns != 2 || r.Size != 64<<10 || len(r.PerConn) != 2 {
		t.Fatalf("unexpected report header: %+v", r)
	}
	if !strings.Contains(stdout, `"conns"`) || !strings.Contains(stdout, `"per_conn"`) {
		t.Fatalf("JSON lacks the conns and per_conn fields:\n%s", stdout)
	}
	if r.Aggregate == nil || r.Aggregate.GBps <= 0 || r.Aggregate.Gbps <= 0 || r.Aggregate.RecordsPerSec <= 0 {
		t.Fatalf("aggregate rates not positive: %+v", r.Aggregate)
	}
	for _, c := range r.PerConn {
		if c.GBps <= 0 {
			t.Fatalf("connection %d rate not positive: %+v", c.Conn, c)
		}
	}
}

func TestFramingJSONCountsRecords(t *testing.T) {
	stdout, stderr, code := runBench(t, "-mode", "framing", "-size", "64B", "-duration", "300ms", "-json")
	if code != 0 {
		t.Fatalf("exit code %d; stderr:\n%s", code, stderr)
	}
	var r report
	if err := json.Unmarshal([]byte(stdout), &r); err != nil {
		t.Fatalf("output is not JSON: %v\n%s", err, stdout)
	}
	if r.Aggregate == nil || r.Aggregate.RecordsPerSec <= 0 {
		t.Fatalf("framing records/s not positive: %+v", r.Aggregate)
	}
}

func TestLatencyJSONReportsPercentiles(t *testing.T) {
	stdout, stderr, code := runBench(t, "-mode", "latency", "-duration", "300ms", "-json")
	if code != 0 {
		t.Fatalf("exit code %d; stderr:\n%s", code, stderr)
	}
	var r report
	if err := json.Unmarshal([]byte(stdout), &r); err != nil {
		t.Fatalf("output is not JSON: %v\n%s", err, stdout)
	}
	l := r.Latency
	if l == nil || l.Samples == 0 || l.P50 <= 0 || l.P99 < l.P50 || l.P999 < l.P99 {
		t.Fatalf("latency percentiles not ordered and positive: %+v", l)
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
	_, err := parseFlags([]string{"-lanes", "2"}, &bytes.Buffer{})
	if err == nil || !strings.Contains(err.Error(), "not defined") {
		t.Fatalf("parseFlags(-lanes 2) = %v, want a flag-not-defined error", err)
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
