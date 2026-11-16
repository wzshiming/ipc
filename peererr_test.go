package ipc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"syscall"
	"testing"
)

func TestPeerErrMapsVanishedPeerToErrPeerClosed(t *testing.T) {
	reset := &net.OpError{Op: "read", Net: "unix", Err: &os.SyscallError{Syscall: "read", Err: syscall.ECONNRESET}}
	for _, err := range []error{fmt.Errorf("recv: %w", io.EOF), fmt.Errorf("recv: %w", io.ErrUnexpectedEOF), reset} {
		if got := peerErr(err); got != ErrPeerClosed {
			t.Errorf("peerErr(%v) = %v, want ErrPeerClosed", err, got)
		}
	}
}

func TestPeerErrPassesUnrelatedErrorsThrough(t *testing.T) {
	for _, err := range []error{errors.New("x"), context.Canceled, os.ErrDeadlineExceeded, net.ErrClosed} {
		if got := peerErr(err); got != err {
			t.Errorf("peerErr(%v) = %v, want it unchanged", err, got)
		}
	}
}
