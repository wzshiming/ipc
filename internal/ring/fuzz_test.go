package ring

import (
	"encoding/binary"
	"errors"
	"sync/atomic"
	"testing"
	"unsafe"
)

// recordHeader returns a record header with payload length n and flags.
func recordHeader(n, flags uint32) []byte {
	return binary.LittleEndian.AppendUint32(binary.LittleEndian.AppendUint32(nil, n), flags)
}

// FuzzConsumer checks that a consumer over cursors and data a hostile producer
// chose, atTail at the tail's offset and atZero at offset 0 where a wrap marker
// leads, never panics, returns no byte outside the data area and fails with
// ErrCorrupt or ErrEmpty only.
func FuzzConsumer(f *testing.F) {
	const last = testCap - recHeader // offset of the data area's last header
	tooLong := uint32(MaxRecord(testCap) + 1)
	var none []byte
	f.Add(uint64(1<<40+24), uint64(1<<40), recordHeader(3, 0), none)                         // two records in a late lap
	f.Add(uint64(last+8+16), uint64(last), recordHeader(0, flagWrap), recordHeader(8, 0))    // a wrap marker at the end
	f.Add(uint64(last+8), uint64(last), recordHeader(0, flagWrap), none)                     // a wrap past the published bytes
	f.Add(uint64(last+16), uint64(last), recordHeader(8, 0), none)                           // a record past the end
	f.Add(uint64(8), uint64(0), recordHeader(0, 2), none)                                    // unknown flags
	f.Add(uint64(recHeader+align8(int(tooLong))), uint64(0), recordHeader(tooLong, 0), none) // a length above MaxRecord
	f.Add(uint64(12), uint64(0), recordHeader(4, 0), none)                                   // a head not a multiple of 8
	f.Add(uint64(56), uint64(64), recordHeader(0, 0), none)                                  // a head behind the tail
	f.Add(uint64(testCap+recHeader), uint64(0), recordHeader(0, 0), none)                    // a head beyond the capacity
	f.Add(uint64(12), uint64(4), recordHeader(0, 0), none)                                   // a tail not a multiple of 8
	f.Fuzz(func(t *testing.T, head, tail uint64, atTail, atZero []byte) {
		region := newRegion(t, testCap)
		data := region[HeaderSize:]
		copy(data, atZero)
		copy(data[tail%testCap:], atTail)
		atomic.StoreUint64(u64(region, offHead), head)
		atomic.StoreUint64(u64(region, offTail), tail)
		c, err := NewConsumer(region)
		if err != nil {
			if !errors.Is(err, ErrCorrupt) {
				t.Fatalf("NewConsumer = %v; want ErrCorrupt", err)
			}
			return
		}
		base := uintptr(unsafe.Pointer(&data[0]))
		for range testCap/recHeader + 1 {
			p, err := c.TryNext()
			if err != nil {
				if !errors.Is(err, ErrCorrupt) && !errors.Is(err, ErrEmpty) {
					t.Fatalf("TryNext = %v; want ErrCorrupt or ErrEmpty", err)
				}
				return
			}
			off := uintptr(unsafe.Pointer(unsafe.SliceData(p))) - base
			if off > testCap || uintptr(cap(p)) > testCap-off {
				t.Fatalf("TryNext returned %d bytes at data offset %d, outside the %d-byte data area", cap(p), int64(off), testCap)
			}
			c.Release()
		}
		t.Fatalf("TryNext returned more records than %d bytes hold", testCap)
	})
}
