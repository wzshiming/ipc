package shm

import (
	"os"
	"testing"
	"unsafe"
)

const helperEnv = "IPC_SHM_TEST_HELPER"

func TestMain(m *testing.M) {
	if role := os.Getenv(helperEnv); role != "" {
		os.Exit(runHelper(role))
	}
	os.Exit(m.Run())
}

func runHelper(role string) int {
	switch role {
	default:
		os.Stderr.WriteString("unknown helper role: " + role + "\n")
		return 2
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
