package ring

import (
	"errors"
	"sync/atomic"
	"unsafe"
)

// HeaderSize is the size of the ring header; the data area starts right after it.
const HeaderSize = 4096

// Each shared header field owns a 128-byte cache line (Apple 128 B, x86 64 B).
const (
	offMeta           = 0 // magic u32, capacity u32
	offHead           = 128
	offTail           = 256
	offConsumerParked = 384
	offProducerParked = 512
)

const (
	magic = 0x474e4952 // "RING" little-endian

	minCapacity = 64 << 10
	maxCapacity = 1 << 30

	recHeader = 8
	flagWrap  = 1
)

var (
	ErrFull     = errors.New("ring: full")
	ErrEmpty    = errors.New("ring: empty")
	ErrTooLarge = errors.New("ring: record too large")
	ErrCorrupt  = errors.New("ring: corrupt record")

	ErrBadMagic         = errors.New("ring: bad magic")
	ErrBadCapacity      = errors.New("ring: capacity must be a power of two in [64 KiB, 1 GiB]")
	ErrCapacityMismatch = errors.New("ring: capacity differs from the expected one")
	ErrShortRegion      = errors.New("ring: region too small")
	ErrUnaligned        = errors.New("ring: region not 8-byte aligned")
)

// Size returns the region size needed for a ring with the given data capacity.
func Size(capacity int) int { return HeaderSize + capacity }

// MaxRecord returns the largest payload a ring of the given capacity accepts.
func MaxRecord(capacity int) int { return capacity/2 - recHeader }

func validCapacity(c int) bool {
	return c >= minCapacity && c <= maxCapacity && c&(c-1) == 0
}

func u32(region []byte, off int) *uint32 { return (*uint32)(unsafe.Pointer(&region[off])) }
func u64(region []byte, off int) *uint64 { return (*uint64)(unsafe.Pointer(&region[off])) }

func aligned8(region []byte) bool { return uintptr(unsafe.Pointer(&region[0]))%8 == 0 }

// Format writes a fresh header for a ring of the given capacity.
func Format(region []byte, capacity int) error {
	if !validCapacity(capacity) {
		return ErrBadCapacity
	}
	if len(region) < Size(capacity) {
		return ErrShortRegion
	}
	if !aligned8(region) {
		return ErrUnaligned
	}
	clear(region[:HeaderSize])
	atomic.StoreUint32(u32(region, offMeta+4), uint32(capacity))
	atomic.StoreUint32(u32(region, offMeta), magic)
	return nil
}

// Verify checks the header and returns the ring's data capacity.
func Verify(region []byte) (capacity int, err error) {
	if len(region) < HeaderSize {
		return 0, ErrShortRegion
	}
	if !aligned8(region) {
		return 0, ErrUnaligned
	}
	if atomic.LoadUint32(u32(region, offMeta)) != magic {
		return 0, ErrBadMagic
	}
	c := int(atomic.LoadUint32(u32(region, offMeta+4)))
	if !validCapacity(c) {
		return 0, ErrBadCapacity
	}
	if len(region)-HeaderSize < c {
		return 0, ErrShortRegion
	}
	return c, nil
}

// view is what both ends of a ring map from its region.
type view struct {
	data           []byte
	sharedHead     *uint64 // written by the producer
	sharedTail     *uint64 // written by the consumer
	consumerParked *uint32
	producerParked *uint32
	mask           uint64
	capacity       uint64
}

// attach verifies that region is formatted for capacity and returns its view.
func attach(region []byte, capacity int) (view, error) {
	c, err := Verify(region)
	if err != nil {
		return view{}, err
	}
	if c != capacity {
		return view{}, ErrCapacityMismatch
	}
	return view{
		data:           region[HeaderSize : HeaderSize+capacity : HeaderSize+capacity],
		sharedHead:     u64(region, offHead),
		sharedTail:     u64(region, offTail),
		consumerParked: u32(region, offConsumerParked),
		producerParked: u32(region, offProducerParked),
		mask:           uint64(capacity - 1),
		capacity:       uint64(capacity),
	}, nil
}
