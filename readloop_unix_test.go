//go:build darwin || linux

package ipc

import (
	"os"
	"testing"
	"time"

	"github.com/wzshiming/ipc/internal/control"
)

// openFDs counts the descriptors this process has open.
func openFDs(t *testing.T) int {
	t.Helper()
	ents, err := os.ReadDir("/dev/fd")
	if err != nil {
		t.Fatal(err)
	}
	return len(ents)
}

func TestDescriptorOnDoorbellAfterHandshakeIsClosedNotAProtocolError(t *testing.T) {
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	defer w.Close()
	var before int
	c, _ := rawPeer(t, func(peer *control.Link) {
		before = openFDs(t)
		bell := doorbell{dirDialerToListener}
		if err := peer.Send(frameDoorbell, bell[:], []int{int(w.Fd())}); err != nil {
			t.Fatal(err)
		}
	})
	select {
	case <-c.rx.sig:
	case <-c.Done():
		t.Fatalf("a descriptor on a doorbell closed the connection: %v", c.Err())
	case <-time.After(5 * time.Second):
		t.Fatal("the doorbell was not delivered")
	}
	if err := c.Err(); err != nil {
		t.Fatalf("Err = %v; want nil, the connection open", err)
	}
	if after := openFDs(t); after != before {
		t.Fatalf("%d descriptors open after a doorbell with one attached; %d before", after, before)
	}
}
