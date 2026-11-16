package ipc

import (
	"errors"
	"testing"

	"github.com/wzshiming/ipc/internal/control"
	"github.com/wzshiming/ipc/internal/ring"
	"github.com/wzshiming/ipc/internal/shm"
)

func TestNewConnReturnsErrCorruptForRingCapacityOtherThanNegotiated(t *testing.T) {
	path := sockPath(t)
	cl, err := control.Listen(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	link, err := control.Dial(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	defer link.Close()
	const rs = 128 << 10
	seg, err := shm.Create(segmentSize(rs))
	if err != nil {
		t.Fatal(err)
	}
	defer seg.Close()
	mem, size := seg.Bytes(), ring.Size(rs)
	for short := range 2 {
		for k := range 2 {
			capacity := rs
			if k == short {
				capacity = rs / 2
			}
			if err := ring.Format(mem[k*size:(k+1)*size], capacity); err != nil {
				t.Fatal(err)
			}
		}
		if c, err := newConn(link, seg, rs, 0, path, false); !errors.Is(err, ErrCorrupt) {
			if c != nil {
				c.Close()
			}
			t.Fatalf("newConn with ring %d formatted for %d bytes = %v; want ErrCorrupt", short, rs/2, err)
		}
	}
}
