package shm

import (
	"os/exec"
	"testing"

	"golang.org/x/sys/windows"
)

func transferFDs(t *testing.T, fds []int) []int { return fds }

func passFDs(t *testing.T, cmd *exec.Cmd, fds []int) []int { return nil }

func TestWindowsNameGoneAfterBothClose(t *testing.T) {
	a, err := Create(MinSize)
	if err != nil {
		t.Fatal(err)
	}
	name := a.sys.name
	b, err := Attach(handOver(t, a))
	if err != nil {
		a.Close()
		t.Fatal(err)
	}

	h, err := openMapping(name)
	if err != nil {
		t.Fatalf("open %q while mapped: %v", name, err)
	}
	windows.CloseHandle(h)

	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	if h, err := openMapping(name); err == nil {
		windows.CloseHandle(h)
		t.Fatalf("open %q succeeded after both Close", name)
	}
}

func TestWindowsAttachMissingNameFails(t *testing.T) {
	name, err := randomName()
	if err != nil {
		t.Fatal(err)
	}
	if seg, err := Attach(Token{Size: MinSize, name: name}); err == nil {
		seg.Close()
		t.Fatalf("Attach(%q) of a nonexistent mapping succeeded", name)
	}
}
