//go:build darwin || linux || windows

package shm

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"unsafe"
)

const (
	helperEnv  = "IPC_SHM_TEST_HELPER"
	payloadEnv = "IPC_SHM_TEST_PAYLOAD"
	fdsEnv     = "IPC_SHM_TEST_FDS"
)

var childPattern = []byte("written-by-child-process")

func TestMain(m *testing.M) {
	if role := os.Getenv(helperEnv); role != "" {
		if err := runHelper(role); err != nil {
			fmt.Fprintln(os.Stderr, "helper:", err)
			os.Exit(1)
		}
		os.Exit(0)
	}
	os.Exit(m.Run())
}

func runHelper(role string) error {
	switch role {
	case "attach-write":
		return helperAttachWrite()
	default:
		return fmt.Errorf("unknown helper role %q", role)
	}
}

func helperAttachWrite() error {
	payload, err := hex.DecodeString(os.Getenv(payloadEnv))
	if err != nil {
		return err
	}
	var fds []int
	if s := os.Getenv(fdsEnv); s != "" {
		for _, f := range strings.Split(s, ",") {
			fd, err := strconv.Atoi(f)
			if err != nil {
				return err
			}
			fds = append(fds, fd)
		}
	}
	tok, err := DecodeToken(payload, fds)
	if err != nil {
		return err
	}
	seg, err := Attach(tok)
	if err != nil {
		return err
	}
	b := seg.Bytes()
	copy(b, childPattern)
	copy(b[len(b)-len(childPattern):], childPattern)
	return seg.Close()
}

// handOver encodes seg's token and decodes it as a peer would receive it.
func handOver(t *testing.T, seg *Segment) Token {
	t.Helper()
	tok, err := seg.Export()
	if err != nil {
		t.Fatal(err)
	}
	defer tok.Close()
	payload, fds := tok.Encode()
	got, err := DecodeToken(payload, transferFDs(t, fds))
	if err != nil {
		t.Fatal(err)
	}
	return got
}

func TestAttachSharesWritesBothWays(t *testing.T) {
	const size = 3 * MinSize
	a, err := Create(size)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	b, err := Attach(handOver(t, a))
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()

	if b.Size() != size || len(b.Bytes()) != size {
		t.Fatalf("attached Size()=%d len=%d, want %d", b.Size(), len(b.Bytes()), size)
	}
	copy(a.Bytes()[100:], "a->b")
	if got := string(b.Bytes()[100:104]); got != "a->b" {
		t.Fatalf("peer read %q, want %q", got, "a->b")
	}
	copy(b.Bytes()[size-4:], "b->a")
	if got := string(a.Bytes()[size-4:]); got != "b->a" {
		t.Fatalf("creator read %q, want %q", got, "b->a")
	}
}

func TestAttachRejectsSizeMismatch(t *testing.T) {
	a, err := Create(1 << 20)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	tok := handOver(t, a)
	tok.Size = 1 << 18
	if b, err := Attach(tok); err == nil {
		b.Close()
		t.Fatal("Attach with a smaller Size than the segment succeeded")
	}
}

func TestCrossProcessChildWritesParentReads(t *testing.T) {
	const size = 4 * MinSize
	seg, err := Create(size)
	if err != nil {
		t.Fatal(err)
	}
	defer seg.Close()
	tok, err := seg.Export()
	if err != nil {
		t.Fatal(err)
	}
	defer tok.Close()
	payload, fds := tok.Encode()

	cmd := exec.Command(os.Args[0], "-test.run=^$")
	childFDs := passFDs(t, cmd, fds)
	fdStrs := make([]string, len(childFDs))
	for i, fd := range childFDs {
		fdStrs[i] = strconv.Itoa(fd)
	}
	cmd.Env = append(os.Environ(),
		helperEnv+"=attach-write",
		payloadEnv+"="+hex.EncodeToString(payload),
		fdsEnv+"="+strings.Join(fdStrs, ","),
	)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("child: %v\n%s", err, out)
	}

	b := seg.Bytes()
	if !bytes.HasPrefix(b, childPattern) {
		t.Fatalf("head = %q, want %q", b[:len(childPattern)], childPattern)
	}
	if !bytes.HasSuffix(b, childPattern) {
		t.Fatalf("tail = %q, want %q", b[len(b)-len(childPattern):], childPattern)
	}
}

func TestCreateBytesLengthIsRequestedSize(t *testing.T) {
	const size = MinSize + 123
	seg, err := Create(size)
	if err != nil {
		t.Fatal(err)
	}
	defer seg.Close()
	if got := len(seg.Bytes()); got != size {
		t.Fatalf("len(Bytes()) = %d, want %d", got, size)
	}
	if got := seg.Size(); got != size {
		t.Fatalf("Size() = %d, want %d", got, size)
	}
}

func TestCreateBaseIsPageAligned(t *testing.T) {
	seg, err := Create(MinSize)
	if err != nil {
		t.Fatal(err)
	}
	defer seg.Close()
	base := uintptr(unsafe.Pointer(unsafe.SliceData(seg.Bytes())))
	if base%uintptr(os.Getpagesize()) != 0 {
		t.Fatalf("base %#x not aligned to page size %d", base, os.Getpagesize())
	}
}

func TestCreateIsZeroFilled(t *testing.T) {
	seg, err := Create(MinSize * 4)
	if err != nil {
		t.Fatal(err)
	}
	defer seg.Close()
	for i, c := range seg.Bytes() {
		if c != 0 {
			t.Fatalf("byte %d = %#x, want 0", i, c)
		}
	}
}

func TestCreateRejectsSizeOutOfRange(t *testing.T) {
	for _, size := range []int{-1, 0, MinSize - 1, MaxSize + 1} {
		if seg, err := Create(size); err == nil {
			seg.Close()
			t.Errorf("Create(%d) succeeded, want error", size)
		}
	}
}

func TestCloseIsIdempotent(t *testing.T) {
	seg, err := Create(MinSize)
	if err != nil {
		t.Fatal(err)
	}
	seg.Bytes()[0] = 1
	if err := seg.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := seg.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	if b := seg.Bytes(); b != nil {
		t.Fatalf("Bytes() after Close = len %d, want nil", len(b))
	}
}
