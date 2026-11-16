package ipc

import (
	"errors"
	"net"
	"testing"
)

func TestListenerSecondCloseReturnsNetErrClosed(t *testing.T) {
	l, _ := listen(t, ListenConfig{})
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	if err := l.Close(); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("second Close: got %v, want net.ErrClosed", err)
	}
}
