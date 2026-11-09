package ipc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"testing"
)

func TestIsPeerGoneMatchesWrappedPeerVanishedErrors(t *testing.T) {
	for _, err := range []error{io.EOF, io.ErrUnexpectedEOF} {
		if !isPeerGone(fmt.Errorf("recv: %w", err)) {
			t.Errorf("isPeerGone(%v) = false, want true", err)
		}
	}
	for _, errno := range peerGoneErrnos {
		err := &net.OpError{Op: "read", Net: "unix", Err: &os.SyscallError{Syscall: "read", Err: errno}}
		if !isPeerGone(err) {
			t.Errorf("isPeerGone(%v) = false, want true", err)
		}
	}
}

func TestIsPeerGoneRejectsUnrelatedErrors(t *testing.T) {
	for _, err := range []error{errors.New("x"), context.Canceled, os.ErrDeadlineExceeded, net.ErrClosed} {
		if isPeerGone(err) {
			t.Errorf("isPeerGone(%v) = true, want false", err)
		}
	}
}
