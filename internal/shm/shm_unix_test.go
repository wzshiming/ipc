//go:build darwin || linux

package shm

import (
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
