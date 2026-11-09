package ring

import (
	"encoding/binary"
	"errors"
	"math/rand/v2"
	"runtime"
	"sync/atomic"
	"testing"
	"time"
)

const testCap = 64 << 10

func newRegion(t testing.TB, capacity int) []byte {
	t.Helper()
	region := make([]byte, Size(capacity))
	if err := Format(region, capacity); err != nil {
		t.Fatalf("Format(%d): %v", capacity, err)
	}
	return region
}

func newPair(t testing.TB, region []byte) (*Producer, *Consumer) {
	t.Helper()
	p, err := NewProducer(region)
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}
	c, err := NewConsumer(region)
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	return p, c
}

func fill(buf []byte, seq uint64) {
	for j := range buf {
		buf[j] = byte(seq*7 + uint64(j))
	}
}

func check(buf []byte, seq uint64) bool {
	for j := range buf {
		if buf[j] != byte(seq*7+uint64(j)) {
			return false
		}
	}
	return true
}

func align8(n int) int { return (n + 7) &^ 7 }

// produce writes one record of n bytes and commits it.
func produce(t testing.TB, p *Producer, n int, seq uint64) {
	t.Helper()
	buf, err := p.TryReserve(n)
	if err != nil {
		t.Fatalf("TryReserve(%d): %v", n, err)
	}
	fill(buf, seq)
	p.Commit()
}

func TestPropertyMatchesReferenceFIFO(t *testing.T) {
	const total = 1_000_000
	region := newRegion(t, testCap)
	p, c := newPair(t, region)
	maxRec := MaxRecord(testCap)
	rng := rand.New(rand.NewPCG(1, 2))

	var fifo []int // sizes of committed, unconsumed records; fifo[i] has seq consumed+i
	var produced, consumed uint64
	var fulls, wraps, exactFills, maxes, zeros int

	consume := func() {
		buf, err := c.TryNext()
		if len(fifo) == 0 {
			if !errors.Is(err, ErrEmpty) {
				t.Fatalf("TryNext on empty reference = %v, want ErrEmpty", err)
			}
			return
		}
		if err != nil {
			t.Fatalf("record %d: TryNext = %v, want %d bytes", consumed, err, fifo[0])
		}
		if len(buf) != fifo[0] || !check(buf, consumed) {
			t.Fatalf("record %d: got %d bytes (payload ok=%v), want %d", consumed, len(buf), check(buf, consumed), fifo[0])
		}
		c.Release()
		fifo = fifo[1:]
		consumed++
	}

	for consumed < total {
		if rng.IntN(2) == 0 {
			consume()
			continue
		}
		off := int(p.head & uint64(testCap-1))
		rem := testCap - off
		free := testCap - int(p.head-c.tail)
		n := rng.IntN(129)
		exact := false
		switch rng.IntN(500) {
		case 0:
			n = 0
		case 1:
			n = maxRec
		case 2:
			if free <= rem && free >= 8 && free-8 <= maxRec {
				n, exact = free-8, true
			}
		case 3:
			if s := rem - 8 + 1 + rng.IntN(64); s <= maxRec {
				n = s
			}
		}
		need := 8 + align8(n)
		if need > rem {
			need += rem
		}
		buf, err := p.TryReserve(n)
		if need > free {
			if !errors.Is(err, ErrFull) {
				t.Fatalf("TryReserve(%d) with %d free (need %d) = %v, want ErrFull", n, free, need, err)
			}
			fulls++
			consume()
			continue
		}
		if err != nil {
			t.Fatalf("TryReserve(%d) with %d free (need %d) = %v", n, free, need, err)
		}
		if len(buf) != n || cap(buf) != n {
			t.Fatalf("TryReserve(%d) returned len %d cap %d", n, len(buf), cap(buf))
		}
		fill(buf, produced)
		p.Commit()
		fifo = append(fifo, n)
		produced++
		if need > 8+align8(n) {
			wraps++
		}
		switch {
		case n == 0:
			zeros++
		case n == maxRec:
			maxes++
		}
		if exact {
			exactFills++
			if _, err := p.TryReserve(0); !errors.Is(err, ErrFull) {
				t.Fatalf("TryReserve(0) after exact fill = %v, want ErrFull", err)
			}
		}
	}
	if _, err := p.TryReserve(maxRec + 1); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("TryReserve(MaxRecord+1) = %v, want ErrTooLarge", err)
	}
	t.Logf("records=%d fulls=%d wraps=%d exactFills=%d max=%d zero=%d", consumed, fulls, wraps, exactFills, maxes, zeros)
	if wraps == 0 || exactFills == 0 || maxes == 0 || zeros == 0 || fulls == 0 {
		t.Fatal("property test did not cover every edge case")
	}
}

func TestPayloadAliasesRegion(t *testing.T) {
	region := newRegion(t, testCap)
	p, c := newPair(t, region)
	buf, err := p.TryReserve(100)
	if err != nil {
		t.Fatal(err)
	}
	if &buf[0] != &region[HeaderSize+8] {
		t.Fatal("TryReserve payload does not alias the data area")
	}
	p.Commit()
	got, err := c.TryNext()
	if err != nil {
		t.Fatal(err)
	}
	if &got[0] != &region[HeaderSize+8] || len(got) != 100 {
		t.Fatal("TryNext payload does not alias the record in the data area")
	}
}

func TestAbortDiscardsReservation(t *testing.T) {
	region := newRegion(t, testCap)
	p, c := newPair(t, region)
	if _, err := p.TryReserve(10); err != nil {
		t.Fatal(err)
	}
	p.Abort()
	produce(t, p, 3, 1)
	buf, err := c.TryNext()
	if err != nil || len(buf) != 3 || !check(buf, 1) {
		t.Fatalf("TryNext after Abort = %d bytes, %v; want the committed 3-byte record", len(buf), err)
	}
	c.Release()
	if _, err := c.TryNext(); !errors.Is(err, ErrEmpty) {
		t.Fatalf("aborted record became visible: %v", err)
	}
}

func TestConcurrentProducerConsumer(t *testing.T) {
	const total = 200_000
	region := newRegion(t, testCap)
	p, c := newPair(t, region)
	maxRec := MaxRecord(testCap)
	done := make(chan struct{})
	go func() {
		defer close(done)
		rng := rand.New(rand.NewPCG(3, 4))
		for seq := uint64(0); seq < total; {
			n := rng.IntN(512)
			if rng.IntN(1000) == 0 {
				n = maxRec
			}
			buf, err := p.TryReserve(n)
			if errors.Is(err, ErrFull) {
				runtime.Gosched()
				continue
			}
			if err != nil {
				t.Errorf("TryReserve: %v", err)
				return
			}
			fill(buf, seq)
			p.Commit()
			seq++
		}
	}()
	for seq := uint64(0); seq < total; {
		buf, err := c.TryNext()
		if errors.Is(err, ErrEmpty) {
			runtime.Gosched()
			continue
		}
		if err != nil {
			t.Fatalf("TryNext: %v", err)
		}
		if !check(buf, seq) {
			t.Fatalf("record %d payload mismatch", seq)
		}
		c.Release()
		seq++
	}
	<-done
}

func TestSteadyStateZeroAllocs(t *testing.T) {
	region := newRegion(t, testCap)
	p, c := newPair(t, region)
	allocs := testing.AllocsPerRun(10000, func() {
		buf, _ := p.TryReserve(64)
		buf[0] = 1
		p.Commit()
		got, _ := c.TryNext()
		_ = got[0]
		c.Release()
	})
	if allocs != 0 {
		t.Fatalf("TryReserve/Commit/TryNext/Release allocate %v per op, want 0", allocs)
	}
}

func dataHeader(region []byte, off int) []byte { return region[HeaderSize+off : HeaderSize+off+8] }

func TestTryNextRejectsTamperedRecords(t *testing.T) {
	maxRec := MaxRecord(testCap)
	cases := []struct {
		name   string
		setup  func(t *testing.T, p *Producer, c *Consumer)
		tamper func(region []byte)
	}{
		{"length above MaxRecord", func(t *testing.T, p *Producer, c *Consumer) { produce(t, p, 16, 0) },
			func(r []byte) { binary.LittleEndian.PutUint32(dataHeader(r, 0), uint32(maxRec+1)) }},
		{"length 0xffffffff", func(t *testing.T, p *Producer, c *Consumer) { produce(t, p, 16, 0) },
			func(r []byte) { binary.LittleEndian.PutUint32(dataHeader(r, 0), 0xffffffff) }},
		{"length beyond published bytes", func(t *testing.T, p *Producer, c *Consumer) { produce(t, p, 16, 0) },
			func(r []byte) { binary.LittleEndian.PutUint32(dataHeader(r, 0), 100) }},
		{"unknown flags", func(t *testing.T, p *Producer, c *Consumer) { produce(t, p, 16, 0) },
			func(r []byte) { binary.LittleEndian.PutUint32(dataHeader(r, 0)[4:], 2) }},
		{"wrap marker skipping unpublished bytes", func(t *testing.T, p *Producer, c *Consumer) { produce(t, p, 0, 0) },
			func(r []byte) { binary.LittleEndian.PutUint32(dataHeader(r, 0)[4:], flagWrap) }},
		{"wrap marker leaving fewer than 8 bytes", func(t *testing.T, p *Producer, c *Consumer) {
			advance(t, p, c, testCap-16)
			produce(t, p, 8, 0) // exactly fills the last 16 bytes
		}, func(r []byte) { binary.LittleEndian.PutUint32(dataHeader(r, testCap-16)[4:], flagWrap) }},
		{"wrap marker followed by oversized record", func(t *testing.T, p *Producer, c *Consumer) {
			advance(t, p, c, testCap-16)
			produce(t, p, 64, 0) // wraps: marker at cap-16, record at 0
		}, func(r []byte) { binary.LittleEndian.PutUint32(dataHeader(r, 0), 1000) }},
		{"wrap marker followed by another marker", func(t *testing.T, p *Producer, c *Consumer) {
			advance(t, p, c, testCap-16)
			produce(t, p, 64, 0)
		}, func(r []byte) { binary.LittleEndian.PutUint32(dataHeader(r, 0)[4:], flagWrap) }},
		{"head beyond capacity", func(t *testing.T, p *Producer, c *Consumer) { produce(t, p, 16, 0) },
			func(r []byte) { atomic.StoreUint64(u64(r, offHead), testCap+8) }},
		{"head not 8-aligned", func(t *testing.T, p *Producer, c *Consumer) {},
			func(r []byte) { atomic.StoreUint64(u64(r, offHead), 3) }},
		{"head behind tail", func(t *testing.T, p *Producer, c *Consumer) {
			produce(t, p, 16, 0)
			mustConsume(t, c)
		}, func(r []byte) { atomic.StoreUint64(u64(r, offHead), 0) }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			region := newRegion(t, testCap)
			p, c := newPair(t, region)
			tc.setup(t, p, c)
			tc.tamper(region)
			buf, err := c.TryNext()
			if !errors.Is(err, ErrCorrupt) {
				t.Fatalf("TryNext = %d bytes, %v; want ErrCorrupt", len(buf), err)
			}
		})
	}
}

func TestTryReserveRejectsTamperedTail(t *testing.T) {
	region := newRegion(t, testCap)
	p, _ := newPair(t, region)
	for {
		if _, err := p.TryReserve(1000); errors.Is(err, ErrFull) {
			break
		} else if err != nil {
			t.Fatal(err)
		}
		p.Commit()
	}
	atomic.StoreUint64(u64(region, offTail), p.head+8)
	if _, err := p.TryReserve(1000); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("TryReserve with tail ahead of head = %v, want ErrCorrupt", err)
	}
}

// advance moves both cursors to data offset off with an empty ring.
func advance(t *testing.T, p *Producer, c *Consumer, off int) {
	t.Helper()
	for int(p.head) < off {
		n := min(off-int(p.head)-8, 1000)
		produce(t, p, n, 0)
		mustConsume(t, c)
	}
	if int(p.head) != off {
		t.Fatalf("advance reached %d, want %d", p.head, off)
	}
}

func mustConsume(t *testing.T, c *Consumer) {
	t.Helper()
	if _, err := c.TryNext(); err != nil {
		t.Fatalf("TryNext: %v", err)
	}
	c.Release()
}

func TestSecondReservePanics(t *testing.T) {
	region := newRegion(t, testCap)
	p, _ := newPair(t, region)
	if _, err := p.TryReserve(8); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if recover() == nil {
			t.Fatal("second TryReserve without Commit/Abort did not panic")
		}
	}()
	p.TryReserve(8)
}

// commit writes one n-byte record and returns Commit's wake result.
func commit(t *testing.T, p *Producer, n int) bool {
	t.Helper()
	buf, err := p.TryReserve(n)
	if err != nil {
		t.Fatalf("TryReserve(%d): %v", n, err)
	}
	fill(buf, 0)
	return p.Commit()
}

// release consumes one record and returns Release's wake result.
func release(t *testing.T, c *Consumer) bool {
	t.Helper()
	if _, err := c.TryNext(); err != nil {
		t.Fatalf("TryNext: %v", err)
	}
	return c.Release()
}

func fillUntilFull(t *testing.T, p *Producer, n int) {
	t.Helper()
	for {
		buf, err := p.TryReserve(n)
		if errors.Is(err, ErrFull) {
			return
		}
		if err != nil {
			t.Fatal(err)
		}
		fill(buf, 0)
		p.Commit()
	}
}

func TestCommitWakesParkedConsumerOnce(t *testing.T) {
	p, c := newPair(t, newRegion(t, testCap))
	if c.Park() {
		t.Fatal("Park on an empty ring reported ready")
	}
	if !commit(t, p, 1) {
		t.Fatal("Commit after the consumer parked did not ask for a doorbell")
	}
	if commit(t, p, 1) {
		t.Fatal("second Commit rang again for the same park")
	}
}

func TestConsumerParkWithDataIsReadyAndClearsFlag(t *testing.T) {
	p, c := newPair(t, newRegion(t, testCap))
	commit(t, p, 1)
	if !c.Park() {
		t.Fatal("Park with a published record did not report ready")
	}
	if commit(t, p, 1) {
		t.Fatal("Commit rang for a park that reported ready")
	}
}

func TestConsumerUnparkIsNotCounted(t *testing.T) {
	p, c := newPair(t, newRegion(t, testCap))
	if c.Park() {
		t.Fatal("Park on an empty ring reported ready")
	}
	c.Unpark()
	if commit(t, p, 1) {
		t.Fatal("Commit rang for a consumer that had unparked")
	}
}

func TestReleaseWakesParkedProducerOnce(t *testing.T) {
	p, c := newPair(t, newRegion(t, testCap))
	fillUntilFull(t, p, 1000)
	if p.Park(1000) {
		t.Fatal("Park on a full ring reported ready")
	}
	if !release(t, c) {
		t.Fatal("Release after the producer parked did not ask for a doorbell")
	}
	if release(t, c) {
		t.Fatal("second Release rang again for the same park")
	}
}

func TestProducerParkWithSpaceIsReadyAndClearsFlag(t *testing.T) {
	p, c := newPair(t, newRegion(t, testCap))
	fillUntilFull(t, p, 1000)
	release(t, c)
	if !p.Park(1000) {
		t.Fatal("Park with room for the record did not report ready")
	}
	if release(t, c) {
		t.Fatal("Release rang for a park that reported ready")
	}
}

func TestProducerUnparkIsNotCounted(t *testing.T) {
	p, c := newPair(t, newRegion(t, testCap))
	fillUntilFull(t, p, 1000)
	if p.Park(1000) {
		t.Fatal("Park on a full ring reported ready")
	}
	p.Unpark()
	if release(t, c) {
		t.Fatal("Release rang for a producer that had unparked")
	}
}

func TestCommitAndReleaseOnUnparkedRingLeaveFlagsClear(t *testing.T) {
	p, c := newPair(t, newRegion(t, testCap))
	if commit(t, p, 1) {
		t.Error("Commit with no parked consumer asked for a doorbell")
	}
	if release(t, c) {
		t.Error("Release with no parked producer asked for a doorbell")
	}
	if f := atomic.LoadUint32(c.consumerParked); f != 0 {
		t.Errorf("consumerParked = %d after Commit, want 0", f)
	}
	if f := atomic.LoadUint32(p.producerParked); f != 0 {
		t.Errorf("producerParked = %d after Release, want 0", f)
	}
}

func TestParkAfterPeerPublishedLeavesFlagClear(t *testing.T) {
	p, c := newPair(t, newRegion(t, testCap))
	commit(t, p, 1)
	if !c.Park() {
		t.Fatal("consumer Park with a published record did not report ready")
	}
	if f := atomic.LoadUint32(c.consumerParked); f != 0 {
		t.Errorf("consumerParked = %d after a ready Park, want 0", f)
	}
	fillUntilFull(t, p, 1000)
	release(t, c)
	if !p.Park(1) {
		t.Fatal("producer Park with room for the record did not report ready")
	}
	if f := atomic.LoadUint32(p.producerParked); f != 0 {
		t.Errorf("producerParked = %d after a ready Park, want 0", f)
	}
}

func TestProducerParkForOversizedRecordDoesNotBlock(t *testing.T) {
	p, _ := newPair(t, newRegion(t, testCap))
	if !p.Park(MaxRecord(testCap) + 1) {
		t.Fatal("Park for a record TryReserve always rejects would block forever")
	}
}

func ring(bell chan struct{}) {
	select {
	case bell <- struct{}{}:
	default:
	}
}

// TestParkProtocolNoLostWakeups runs a producer and a consumer that park on a
// full/empty ring and wait on channel doorbells until 1e6 waits have happened.
// A lost wakeup deadlocks and trips the watchdog. Opening phases force the
// consumer to block on the empty ring, then the producer on the full one.
func TestParkProtocolNoLostWakeups(t *testing.T) {
	const wantWaits = 1_000_000
	const sentinel = 65 // payload sizes are otherwise 0..64
	p, c := newPair(t, newRegion(t, testCap))
	prodBell := make(chan struct{}, 1)
	consBell := make(chan struct{}, 1)
	consBlocked := make(chan struct{})
	prodBlocked := make(chan struct{})
	var waits atomic.Int64
	var prodParks, prodRings, consParks, consRings, unparks int
	done := make(chan struct{}, 2)

	go func() {
		defer func() { done <- struct{}{} }()
		<-consBlocked
		rng := rand.New(rand.NewPCG(5, 6))
		for seq := uint64(0); ; {
			n := rng.IntN(sentinel)
			last := waits.Load() >= wantWaits
			if last {
				n = sentinel
			}
			buf, err := p.TryReserve(n)
			if errors.Is(err, ErrFull) {
				waits.Add(1)
				prodParks++
				if p.Park(n) {
					continue
				}
				if prodParks == 1 {
					close(prodBlocked)
				}
				if rng.IntN(16) == 0 {
					p.Unpark()
					unparks++
					continue
				}
				<-prodBell
				continue
			}
			if err != nil {
				t.Errorf("TryReserve: %v", err)
				return
			}
			fill(buf, seq)
			if p.Commit() {
				consRings++
				ring(consBell)
			}
			seq++
			if last {
				return
			}
			if rng.IntN(8) == 0 {
				runtime.Gosched()
			}
		}
	}()
	go func() {
		defer func() { done <- struct{}{} }()
		waits.Add(1)
		consParks++
		if c.Park() {
			t.Error("consumer Park on the untouched ring reported ready")
			return
		}
		close(consBlocked)
		<-consBell
		<-prodBlocked
		rng := rand.New(rand.NewPCG(7, 8))
		for seq := uint64(0); ; {
			buf, err := c.TryNext()
			if errors.Is(err, ErrEmpty) {
				waits.Add(1)
				consParks++
				if c.Park() {
					continue
				}
				if rng.IntN(16) == 0 {
					c.Unpark()
					continue
				}
				<-consBell
				continue
			}
			if err != nil {
				t.Errorf("TryNext: %v", err)
				return
			}
			if !check(buf, seq) {
				t.Errorf("record %d payload mismatch", seq)
				return
			}
			last := len(buf) == sentinel
			if c.Release() {
				prodRings++
				ring(prodBell)
			}
			seq++
			if last {
				return
			}
			if rng.IntN(8) == 0 {
				runtime.Gosched()
			}
		}
	}()
	for range 2 {
		select {
		case <-done:
		case <-time.After(2 * time.Minute):
			t.Fatalf("deadlock: no progress after %d waits", waits.Load())
		}
	}
	t.Logf("waits=%d producer parks=%d rings=%d consumer parks=%d rings=%d producer unparks=%d",
		waits.Load(), prodParks, prodRings, consParks, consRings, unparks)
	if consRings > consParks {
		t.Errorf("consumer woken %d times for %d parks", consRings, consParks)
	}
	if prodRings > prodParks {
		t.Errorf("producer woken %d times for %d parks", prodRings, prodParks)
	}
	if prodParks == 0 || consParks == 0 {
		t.Error("schedule never parked one side")
	}
}

func TestHeaderFieldsOnDistinctCacheLines(t *testing.T) {
	if HeaderSize != 4096 {
		t.Fatalf("HeaderSize = %d, want 4096", HeaderSize)
	}
	offs := map[string]int{
		"meta":           offMeta,
		"head":           offHead,
		"tail":           offTail,
		"consumerParked": offConsumerParked,
		"producerParked": offProducerParked,
	}
	lines := map[int]string{}
	for name, off := range offs {
		if off%128 != 0 {
			t.Errorf("%s offset %d is not 128-byte aligned", name, off)
		}
		if off+128 > HeaderSize {
			t.Errorf("%s offset %d leaves the header", name, off)
		}
		if other, ok := lines[off/128]; ok {
			t.Errorf("%s shares cache line %d with %s", name, off/128, other)
		}
		lines[off/128] = name
	}
}

func TestSizeIsHeaderPlusCapacity(t *testing.T) {
	if got := Size(testCap); got != HeaderSize+testCap {
		t.Fatalf("Size(%d) = %d, want %d", testCap, got, HeaderSize+testCap)
	}
	if got := MaxRecord(testCap); got != testCap/2-8 {
		t.Fatalf("MaxRecord(%d) = %d, want %d", testCap, got, testCap/2-8)
	}
}

func TestFormatThenVerifyReturnsCapacity(t *testing.T) {
	region := newRegion(t, testCap)
	got, err := Verify(region)
	if err != nil || got != testCap {
		t.Fatalf("Verify = %d, %v; want %d, nil", got, err, testCap)
	}
}

func TestFormatRejectsBadCapacityAndShortRegion(t *testing.T) {
	for _, c := range []int{0, -testCap, 32 << 10, 64<<10 + 8, 3 << 16} {
		if err := Format(make([]byte, HeaderSize+64<<10), c); err == nil {
			t.Errorf("Format(capacity=%d) succeeded, want error", c)
		}
	}
	if err := Format(make([]byte, Size(testCap)-1), testCap); err == nil {
		t.Error("Format on a short region succeeded, want error")
	}
}

func TestVerifyRejectsTamperedHeader(t *testing.T) {
	cases := []struct {
		name   string
		tamper func(r []byte)
	}{
		{"magic", func(r []byte) { r[offMeta] ^= 0xff }},
		{"version", func(r []byte) { binary.LittleEndian.PutUint32(r[offMeta+4:], 99) }},
		{"capacity not power of two", func(r []byte) { binary.LittleEndian.PutUint64(r[offMeta+8:], testCap+8) }},
		{"capacity too small", func(r []byte) { binary.LittleEndian.PutUint64(r[offMeta+8:], 32<<10) }},
		{"capacity too large", func(r []byte) { binary.LittleEndian.PutUint64(r[offMeta+8:], 1<<40) }},
		{"capacity beyond region", func(r []byte) { binary.LittleEndian.PutUint64(r[offMeta+8:], 2*testCap) }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			region := newRegion(t, testCap)
			tc.tamper(region)
			if c, err := Verify(region); err == nil {
				t.Fatalf("Verify accepted tampered header (capacity %d)", c)
			}
		})
	}
	if _, err := Verify(make([]byte, HeaderSize-1)); err == nil {
		t.Error("Verify accepted a region shorter than the header")
	}
}
