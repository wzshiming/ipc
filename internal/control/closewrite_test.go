package control

import (
	"errors"
	"io"
	"testing"
	"time"
)

func TestCloseWriteEndsThePeersStreamAfterTheFramesSentAndRecvGoesOn(t *testing.T) {
	a, b := pair(t)
	if err := a.TrySend(7, []byte("bye")); err != nil {
		t.Fatal(err)
	}
	if err := a.CloseWrite(); err != nil {
		t.Fatal("CloseWrite:", err)
	}
	b.SetReadDeadline(time.Now().Add(5 * time.Second))
	if typ, p, _, err := b.Recv(); err != nil || typ != 7 || string(p) != "bye" {
		t.Fatalf("the peer's Recv = %d %q, %v; want the frame sent before CloseWrite", typ, p, err)
	}
	if _, _, _, err := b.Recv(); err != io.EOF {
		t.Fatalf("the peer's next Recv = %v; want io.EOF", err)
	}
	if err := b.TrySend(8, []byte("late")); err != nil {
		t.Fatalf("the peer's TrySend after its io.EOF = %v; want the frame taken", err)
	}
	a.SetReadDeadline(time.Now().Add(5 * time.Second))
	if typ, p, _, err := a.Recv(); err != nil || typ != 8 || string(p) != "late" {
		t.Fatalf("Recv after CloseWrite = %d %q, %v; want the peer's frame", typ, p, err)
	}
}

func TestCloseWriteAfterCloseIsErrClosed(t *testing.T) {
	a, _ := pair(t)
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	if err := a.CloseWrite(); !errors.Is(err, ErrClosed) {
		t.Fatalf("got %v, want ErrClosed", err)
	}
}
