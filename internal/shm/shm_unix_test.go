//go:build darwin || linux

package shm

import (
	"encoding/binary"
	"os"
	"os/exec"
	"testing"

	"golang.org/x/sys/unix"
)

// transferFDs dups fds as SCM_RIGHTS would, leaving the originals to their owner.
func transferFDs(t *testing.T, fds []int) []int {
	t.Helper()
	out := make([]int, len(fds))
	for i, fd := range fds {
		nfd, err := unix.Dup(fd)
		if err != nil {
			t.Fatal(err)
		}
		out[i] = nfd
	}
	return out
}

// passFDs hands dups of fds to cmd as ExtraFiles and returns their numbers in the child.
func passFDs(t *testing.T, cmd *exec.Cmd, fds []int) []int {
	t.Helper()
	child := make([]int, len(fds))
	for i, fd := range transferFDs(t, fds) {
		f := os.NewFile(uintptr(fd), "shm")
		t.Cleanup(func() { f.Close() })
		cmd.ExtraFiles = append(cmd.ExtraFiles, f)
		child[i] = 3 + i
	}
	return child
}

func TestDecodeRejectsWrongFDCount(t *testing.T) {
	seg, err := Create(MinSize)
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

	for _, n := range []int{0, 2} {
		var in []int
		for range n {
			in = append(in, transferFDs(t, fds)...)
		}
		if got, err := DecodeToken(payload, in); err == nil {
			got.Close()
			t.Errorf("DecodeToken accepted %d fds", n)
		}
	}
}

func TestDecodeClosesFDsOnWrongCount(t *testing.T) {
	payload := binary.LittleEndian.AppendUint64(nil, MinSize)
	fds := transferFDs(t, []int{0, 0})
	if got, err := DecodeToken(payload, fds); err == nil {
		got.Close()
		t.Fatal("DecodeToken accepted 2 fds")
	}
	for _, fd := range fds {
		if _, err := unix.FcntlInt(uintptr(fd), unix.F_GETFD, 0); err != unix.EBADF {
			t.Errorf("fd %d still open after failed decode (err=%v)", fd, err)
		}
	}
}
