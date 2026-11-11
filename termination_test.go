package ipc

import (
	"io"
	"net"
	"testing"
)

func TestCommitAfterLocalClosePublishesNothing(t *testing.T) {
	lc, dc := pair(t, ListenConfig{}, Dialer{})
	p, err := dc.TryReserve(5)
	if err != nil {
		t.Fatal(err)
	}
	copy(p, "ghost")
	dc.Close()
	if err := dc.Commit(); err != net.ErrClosed {
		t.Fatalf("Commit after Close = %v; want net.ErrClosed", err)
	}
	<-lc.Done()
	if rec, err := lc.TryNext(); err != io.EOF {
		t.Fatalf("the peer's TryNext = %q, %v; want io.EOF, no record", rec, err)
	}
}
