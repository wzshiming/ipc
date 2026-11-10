package ipc

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/wzshiming/ipc/internal/control"
	"github.com/wzshiming/ipc/internal/ring"
	"github.com/wzshiming/ipc/internal/shm"
)

// rawPeer returns the listener side of a connection whose peer is the bare
// control link returned with it. No handshake runs: queue sends on the peer
// before the connection's reader starts, so its frames wait in the socket
// for the reader's first read.
func rawPeer(t *testing.T, queue func(peer *control.Link)) (*Conn, *control.Link) {
	t.Helper()
	path := sockPath(t)
	cl, err := control.Listen(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cl.Close() })
	ch := make(chan *control.Link, 1)
	go func() {
		link, _ := cl.Accept()
		ch <- link
	}()
	peer, err := control.Dial(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { peer.Close() })
	link := <-ch
	if link == nil {
		t.Fatal("Accept failed")
	}
	const rs = 64 << 10
	seg, err := shm.Create(int(segmentSize(rs)))
	if err != nil {
		link.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { seg.Close() })
	mem, size := seg.Bytes(), ring.Size(rs)
	for k := range 2 {
		if err := ring.Format(mem[k*size:(k+1)*size], rs); err != nil {
			link.Close()
			t.Fatal(err)
		}
	}
	queue(peer)
	c, err := newConn(link, seg, rs, 0, path, true)
	if err != nil {
		link.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return c, peer
}

func TestDoorbellAndByeInOneReadBothTakeEffect(t *testing.T) {
	c, _ := rawPeer(t, func(peer *control.Link) {
		bell := doorbell{dirDialerToListener}
		if err := peer.Send(frameDoorbell, bell[:], nil); err != nil {
			t.Fatal(err)
		}
		if err := peer.Send(frameBye, nil, nil); err != nil {
			t.Fatal(err)
		}
	})
	select {
	case <-c.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("Bye did not close the connection")
	}
	if !errors.Is(c.Err(), ErrPeerClosed) {
		t.Fatalf("Err = %v; want ErrPeerClosed", c.Err())
	}
	if _, err := c.TryNext(); err != io.EOF {
		t.Fatalf("TryNext = %v; want io.EOF, the Bye's mark", err)
	}
	select {
	case <-c.rx.sig:
	default:
		t.Fatal("the doorbell read along with the Bye left no signal on the receive half")
	}
}
