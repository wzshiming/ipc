package control

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"testing"
)

func TestPeerGoneMatchesWrappedErrnos(t *testing.T) {
	for _, errno := range peerGoneErrnos {
		err := &net.OpError{Op: "read", Net: "unix", Err: &os.SyscallError{Syscall: "read", Err: errno}}
		if !PeerGone(err) {
			t.Errorf("PeerGone(%v) = false, want true", err)
		}
	}
}

func TestPeerGoneRejectsEOFAndUnrelatedErrors(t *testing.T) {
	for _, err := range []error{io.EOF, io.ErrUnexpectedEOF, errors.New("x"), context.Canceled, os.ErrDeadlineExceeded, net.ErrClosed} {
		if PeerGone(err) {
			t.Errorf("PeerGone(%v) = true, want false", err)
		}
	}
}
