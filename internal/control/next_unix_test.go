//go:build darwin || linux

package control

import (
	"os"
	"testing"
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

func TestNextLeavesNoDescriptorOpen(t *testing.T) {
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	defer w.Close()

	a, b := pair(t)
	before := openFDs(t)
	if err := a.Send(1, []byte("fd"), []int{int(w.Fd())}); err != nil {
		t.Fatal(err)
	}
	if err := a.Send(2, []byte("queued"), nil); err != nil {
		t.Fatal(err)
	}
	for i, want := range []string{"fd", "queued"} {
		typ, p, err := b.Next()
		if err != nil || typ != uint8(i+1) || string(p) != want {
			t.Fatalf("frame %d: typ=%d payload=%q err=%v", i, typ, p, err)
		}
	}
	if after := openFDs(t); after != before {
		t.Fatalf("%d descriptors open after a frame with one attached; %d before", after, before)
	}
	if err := a.Send(3, []byte("later"), nil); err != nil {
		t.Fatal(err)
	}
	if typ, p, err := b.Next(); err != nil || typ != 3 || string(p) != "later" {
		t.Fatalf("frame after the attachment: typ=%d payload=%q err=%v", typ, p, err)
	}
}
