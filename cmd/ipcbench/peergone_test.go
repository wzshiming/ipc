package main

import (
	"fmt"
	"net"
	"os"
	"testing"
)

// The peer's socket calls fail this way once the parent has closed first.
func TestPeerGoneMatchesWrappedSocketErrors(t *testing.T) {
	for _, errno := range peerGoneErrnos {
		err := fmt.Errorf("connection 0: %w", &net.OpError{Op: "write", Net: "unix", Err: &os.SyscallError{Syscall: "write", Err: errno}})
		if !peerGone(err) {
			t.Errorf("peerGone(%v) = false, want true", err)
		}
	}
}
