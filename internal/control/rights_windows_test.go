//go:build windows

package control

import (
	"errors"
	"testing"
)

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
