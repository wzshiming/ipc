package ring

import (
	"errors"
	"math/rand/v2"
	"runtime"
	"testing"
)

// commitLen draws how many of n reserved bytes to commit, favouring 0 and n.
func commitLen(rng *rand.Rand, n int) int {
	switch rng.IntN(4) {
	case 0:
		return 0
	case 1:
		return n
	}
	return rng.IntN(n + 1)
}

func TestPropertyCommitNMatchesReferenceFIFO(t *testing.T) {
	const total = 1_000_000
	region := newRegion(t, testCap)
	p, c := newPair(t, region)
	maxRec := MaxRecord(testCap)
	rng := rand.New(rand.NewPCG(5, 6))

	var fifo []int // committed lengths of unconsumed records; fifo[i] has seq consumed+i
	var produced, consumed uint64
	var wraps, shortened, aborts int

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
		n := rng.IntN(129)
		switch rng.IntN(100) {
		case 0:
			n = 0
		case 1:
			n = maxRec
		case 2:
			if s := rem - 8 + 1 + rng.IntN(64); s <= maxRec {
				n = s
			}
		}
		buf, err := p.TryReserve(n)
		if errors.Is(err, ErrFull) {
			consume()
			continue
		}
		if err != nil {
			t.Fatalf("TryReserve(%d): %v", n, err)
		}
		fill(buf, produced)
		wrapped := 8+align8(n) > rem
		m := n
		switch rng.IntN(3) {
		case 0:
			p.Commit()
		case 1:
			m = commitLen(rng, n)
			p.CommitN(m)
		default:
			p.Abort()
			aborts++
			continue
		}
		fifo = append(fifo, m)
		produced++
		if wrapped {
			wraps++
		}
		if m < n {
			shortened++
		}
	}
	t.Logf("records=%d wraps=%d shortened=%d aborts=%d", consumed, wraps, shortened, aborts)
	if wraps == 0 || shortened == 0 || aborts == 0 {
		t.Fatal("property test did not cover wraps, shortened commits and aborts")
	}
}

func TestCommitNZeroLeavesEmptyRecord(t *testing.T) {
	p, c := newPair(t, newRegion(t, testCap))
	if _, err := p.TryReserve(100); err != nil {
		t.Fatal(err)
	}
	p.CommitN(0)
	if p.head != 8 {
		t.Fatalf("head after CommitN(0) = %d, want 8", p.head)
	}
	produce(t, p, 5, 1)
	if buf, err := c.TryNext(); err != nil || len(buf) != 0 {
		t.Fatalf("first record = %d bytes, %v; want the empty record", len(buf), err)
	}
	c.Release()
	if buf, err := c.TryNext(); err != nil || len(buf) != 5 || !check(buf, 1) {
		t.Fatalf("second record = %d bytes, %v; want the 5-byte record", len(buf), err)
	}
}

func TestCommitNMisusePanics(t *testing.T) {
	cases := []struct {
		name    string
		reserve bool
		n       int
	}{
		{"n above reserved length", true, 11},
		{"negative n", true, -1},
		{"no pending reservation", false, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p, _ := newPair(t, newRegion(t, testCap))
			if tc.reserve {
				if _, err := p.TryReserve(10); err != nil {
					t.Fatal(err)
				}
			}
			defer func() {
				if recover() == nil {
					t.Fatalf("CommitN(%d) did not panic", tc.n)
				}
			}()
			p.CommitN(tc.n)
		})
	}
}

func TestConcurrentCommitAndCommitN(t *testing.T) {
	const total = 200_000
	p, c := newPair(t, newRegion(t, testCap))
	maxRec := MaxRecord(testCap)
	// Both sides draw the same (n, m) sequence so the consumer knows each length.
	draw := func(rng *rand.Rand) (n, m int, short bool) {
		n = rng.IntN(512)
		if rng.IntN(1000) == 0 {
			n = maxRec
		}
		if rng.IntN(2) == 0 {
			return n, n, false
		}
		return n, commitLen(rng, n), true
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		rng := rand.New(rand.NewPCG(7, 8))
		for seq := uint64(0); seq < total; seq++ {
			n, m, short := draw(rng)
			for {
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
				break
			}
			if short {
				p.CommitN(m)
			} else {
				p.Commit()
			}
		}
	}()
	rng := rand.New(rand.NewPCG(7, 8))
	for seq := uint64(0); seq < total; {
		buf, err := c.TryNext()
		if errors.Is(err, ErrEmpty) {
			runtime.Gosched()
			continue
		}
		if err != nil {
			t.Fatalf("TryNext: %v", err)
		}
		_, m, _ := draw(rng)
		if len(buf) != m || !check(buf, seq) {
			t.Fatalf("record %d: got %d bytes (payload ok=%v), want %d", seq, len(buf), check(buf, seq), m)
		}
		c.Release()
		seq++
	}
	<-done
}
