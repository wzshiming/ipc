package ring

import (
	"encoding/binary"
	"sync/atomic"
)

// Consumer is the reading end of a ring. It must be used by one goroutine only.
// It trusts nothing the producer wrote: every length is checked against
// MaxRecord, the published bytes and the end of the data area.
type Consumer struct {
	data           []byte
	sharedHead     *uint64 // written by the producer
	sharedTail     *uint64 // written by us
	consumerParked *uint32
	producerParked *uint32
	mask           uint64
	capacity       uint64
	maxRecord      uint64
	tail           uint64 // local copy of the shared tail
	cachedHead     uint64
	pending        uint64 // bytes the current record advances tail by; 0 if none
}

// NewConsumer attaches a consumer to a formatted region.
func NewConsumer(region []byte) (*Consumer, error) {
	capacity, err := Verify(region)
	if err != nil {
		return nil, err
	}
	c := &Consumer{
		data:           region[HeaderSize : HeaderSize+capacity : HeaderSize+capacity],
		sharedHead:     u64(region, offHead),
		sharedTail:     u64(region, offTail),
		consumerParked: u32(region, offConsumerParked),
		producerParked: u32(region, offProducerParked),
		mask:           uint64(capacity - 1),
		capacity:       uint64(capacity),
		maxRecord:      uint64(MaxRecord(capacity)),
	}
	c.tail = atomic.LoadUint64(c.sharedTail)
	c.cachedHead = c.tail
	if c.tail%recHeader != 0 {
		return nil, ErrCorrupt
	}
	return c, nil
}

// available returns the published bytes past tail, reading the shared head
// only when the cached one shows less than a record header.
func (c *Consumer) available() (uint64, error) {
	avail := c.cachedHead - c.tail
	if avail >= recHeader && avail <= c.capacity {
		return avail, nil
	}
	c.cachedHead = atomic.LoadUint64(c.sharedHead)
	avail = c.cachedHead - c.tail
	switch {
	case avail == 0:
		return 0, ErrEmpty
	case avail < recHeader || avail > c.capacity || avail%recHeader != 0:
		return 0, ErrCorrupt
	}
	return avail, nil
}

// TryNext returns the payload of the next record without blocking. The slice
// stays valid until Release.
func (c *Consumer) TryNext() ([]byte, error) {
	if c.pending != 0 {
		panic("ring: TryNext before Release of the previous record")
	}
	avail, err := c.available()
	if err != nil {
		return nil, err
	}
	off := c.tail & c.mask
	hdr := binary.LittleEndian.Uint64(c.data[off:])
	var skipped uint64
	if hdr>>32 == flagWrap {
		skipped = c.capacity - off
		if skipped+recHeader > avail {
			return nil, ErrCorrupt
		}
		off = 0
		hdr = binary.LittleEndian.Uint64(c.data[0:])
	}
	if hdr>>32 != 0 {
		return nil, ErrCorrupt
	}
	n := hdr & 0xffffffff
	if n > c.maxRecord {
		return nil, ErrCorrupt
	}
	rec := recHeader + (n+7)&^7
	if skipped+rec > avail || off+rec > c.capacity {
		return nil, ErrCorrupt
	}
	c.pending = skipped + rec
	start := off + recHeader
	end := start + n
	return c.data[start:end:end], nil
}

// Release frees the current record and reports whether the producer was
// parked and must be woken.
func (c *Consumer) Release() (wakeProducer bool) {
	if c.pending == 0 {
		panic("ring: Release without a record")
	}
	c.tail += c.pending
	c.pending = 0
	atomic.StoreUint64(c.sharedTail, c.tail)
	return atomic.LoadUint32(c.producerParked) != 0 && atomic.SwapUint32(c.producerParked, 0) != 0
}

// Park marks the consumer as waiting for a record, then re-checks. It returns
// true, with the mark cleared, when a record is already published; otherwise
// the caller waits for a doorbell from the producer.
func (c *Consumer) Park() (ready bool) {
	atomic.StoreUint32(c.consumerParked, 1)
	if atomic.LoadUint64(c.sharedHead) != c.tail {
		c.Unpark()
		return true
	}
	return false
}

// Unpark clears the waiting mark when the caller gives up waiting.
func (c *Consumer) Unpark() { atomic.StoreUint32(c.consumerParked, 0) }
