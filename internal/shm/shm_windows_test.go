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

func TestWindowsDecodeRejectsFDs(t *testing.T) {
	seg, err := Create(MinSize)
	if err != nil {
		t.Fatal(err)
	}
	defer seg.Close()
	tok, err := seg.Export()
	if err != nil {
		t.Fatal(err)
	}
	payload, _ := tok.Encode()
	if _, err := DecodeToken(payload, []int{3}); err == nil {
		t.Fatal("DecodeToken accepted an fd on Windows")
	}
}

func TestWindowsDecodeRejectsBadName(t *testing.T) {
	for _, name := range []string{"", `Global\ipc-0123456789abcdef0123456789abcdef`, `Local\ipc-zz`} {
		payload := Token{Size: MinSize, name: name}
		p, _ := payload.Encode()
		if _, err := DecodeToken(p, nil); err == nil {
			t.Errorf("DecodeToken accepted name %q", name)
		}
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
