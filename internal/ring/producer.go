package ring

import (
	"encoding/binary"
	"sync/atomic"
)

// Producer is the writing end of a ring. It must be used by one goroutine only.
type Producer struct {
	data           []byte
	sharedHead     *uint64 // written by us
	sharedTail     *uint64 // written by the consumer
	consumerParked *uint32
	producerParked *uint32
	mask       uint64
	capacity   uint64
	maxRecord  int
	head       uint64 // local copy of the shared head
	cachedTail uint64
	pending    uint64 // bytes the outstanding reservation advances head by; 0 if none
}

// NewProducer attaches a producer to a formatted region.
func NewProducer(region []byte) (*Producer, error) {
	capacity, err := Verify(region)
	if err != nil {
		return nil, err
	}
	p := &Producer{
		data:       region[HeaderSize : HeaderSize+capacity : HeaderSize+capacity],
		sharedHead: u64(region, offHead),
		sharedTail:     u64(region, offTail),
		consumerParked: u32(region, offConsumerParked),
		producerParked: u32(region, offProducerParked),
		mask:       uint64(capacity - 1),
		capacity:   uint64(capacity),
		maxRecord:  MaxRecord(capacity),
	}
	p.head = atomic.LoadUint64(p.sharedHead)
	p.cachedTail = atomic.LoadUint64(p.sharedTail)
	if p.head%recHeader != 0 || p.head-p.cachedTail > p.capacity {
		return nil, ErrCorrupt
	}
	return p, nil
}

// need returns the bytes a record of n payload bytes advances head by, which
// includes the bytes a wrap marker skips when the record does not fit before
// the end of the data area.
func (p *Producer) need(n int) (need uint64, wrap bool) {
	rec := recHeader + (uint64(n)+7)&^7
	rem := p.capacity - p.head&p.mask
	if rec > rem {
		return rem + rec, true
	}
	return rec, false
}

// fits reports whether need bytes are free, reading the shared tail only when
// the cached one says they are not.
func (p *Producer) fits(need uint64) (bool, error) {
	if p.capacity-(p.head-p.cachedTail) >= need {
		return true, nil
	}
	p.cachedTail = atomic.LoadUint64(p.sharedTail)
	used := p.head - p.cachedTail
	if used > p.capacity {
		return false, ErrCorrupt
	}
	return p.capacity-used >= need, nil
}

// TryReserve returns an n-byte payload slice inside the ring without blocking.
// Only one reservation may be outstanding; finish it with Commit or Abort.
func (p *Producer) TryReserve(n int) ([]byte, error) {
	if p.pending != 0 {
		panic("ring: TryReserve with an outstanding reservation")
	}
	if n < 0 || n > p.maxRecord {
		return nil, ErrTooLarge
	}
	need, wrap := p.need(n)
	ok, err := p.fits(need)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, ErrFull
	}
	off := p.head & p.mask
	if wrap {
		binary.LittleEndian.PutUint64(p.data[off:], flagWrap<<32)
		off = 0
	}
	binary.LittleEndian.PutUint64(p.data[off:], uint64(n))
	p.pending = need
	start := off + recHeader
	end := start + uint64(n)
	return p.data[start:end:end], nil
}

// Commit publishes the reserved record and reports whether the consumer was
// parked and must be woken.
func (p *Producer) Commit() (wakeConsumer bool) {
	if p.pending == 0 {
		panic("ring: Commit without a reservation")
	}
	p.head += p.pending
	p.pending = 0
	atomic.StoreUint64(p.sharedHead, p.head)
	return atomic.LoadUint32(p.consumerParked) != 0 && atomic.SwapUint32(p.consumerParked, 0) != 0
}

// Abort discards the outstanding reservation, if any.
func (p *Producer) Abort() { p.pending = 0 }

// Park marks the producer as waiting for room for an n-byte record, then
// re-checks. It returns true, with the mark cleared, when the record already
// fits (or never will: n > MaxRecord); otherwise the caller waits for a
// doorbell from the consumer.
func (p *Producer) Park(n int) (ready bool) {
	if n < 0 || n > p.maxRecord {
		return true
	}
	atomic.StoreUint32(p.producerParked, 1)
	need, _ := p.need(n)
	if ok, err := p.fits(need); ok || err != nil {
		p.Unpark()
		return true
	}
	return false
}

// Unpark clears the waiting mark when the caller gives up waiting.
func (p *Producer) Unpark() { atomic.StoreUint32(p.producerParked, 0) }
