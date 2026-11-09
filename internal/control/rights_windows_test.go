//go:build windows

package control

import (
	"errors"
	"net"
	"os"
	"testing"

	"golang.org/x/sys/windows"
)

func TestNoAFUnixErrorWrapsErrUnsupported(t *testing.T) {
	sys := &net.OpError{Op: "listen", Net: "unix", Err: os.NewSyscallError("socket", windows.WSAEAFNOSUPPORT)}
	err := unsupportedErr(sys)
	if !errors.Is(err, ErrUnsupported) || !errors.Is(err, windows.WSAEAFNOSUPPORT) {
		t.Fatalf("got %v, want ErrUnsupported wrapping WSAEAFNOSUPPORT", err)
	}
	other := &net.OpError{Op: "listen", Net: "unix", Err: windows.WSAEADDRINUSE}
	if err := unsupportedErr(other); err != other {
		t.Fatalf("unrelated error rewritten: %v", err)
	}
}

func TestSendWithFDsIsUnsupportedOnWindows(t *testing.T) {
	a, _ := pair(t)
	if err := a.Send(1, []byte("x"), []int{1}); !errors.Is(err, ErrUnsupported) {
		t.Fatalf("got %v, want ErrUnsupported", err)
	}
}

func TestSendWithoutFDsWorksOnWindows(t *testing.T) {
	a, b := pair(t)
	if err := a.Send(3, []byte("win"), nil); err != nil {
		t.Fatal(err)
	}
	typ, p, fds, err := b.Recv()
	if err != nil || typ != 3 || string(p) != "win" || fds != nil {
		t.Fatalf("typ=%d payload=%q fds=%v err=%v", typ, p, fds, err)
	}
}
