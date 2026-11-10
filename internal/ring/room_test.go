package ring

import (
	"errors"
	"math/rand/v2"
	"testing"
)

func TestRoomIsTheLargestPayloadTryReserveAccepts(t *testing.T) {
	p, c := newPair(t, newRegion(t, testCap))
	maxRec := MaxRecord(testCap)
	rng := rand.New(rand.NewPCG(9, 10))
	var queued, wrapped, capped, tight int
	filling := true
	for range 20_000 {
		room := p.Room()
		if room > 0 {
			if _, err := p.TryReserve(room); err != nil {
				t.Fatalf("TryReserve(Room() = %d) = %v", room, err)
			}
			p.Abort()
		}
		if _, err := p.TryReserve(room + 1); !errors.Is(err, ErrFull) && !errors.Is(err, ErrTooLarge) {
			t.Fatalf("TryReserve(Room()+1 = %d) = %v; want ErrFull or ErrTooLarge", room+1, err)
		}
		rem := testCap - int(p.head&uint64(testCap-1))
		switch {
		case room == maxRec:
			capped++
		case room+8 > rem:
			wrapped++
		case room < 4096:
			tight++
		}
		// Fill the ring until little is left, drain it, repeat, with a
		// quarter of the steps going the other way.
		if queued == 0 {
			filling = true
		} else if room < 1000 {
			filling = false
		}
		if filling == (rng.IntN(4) != 0) && room > 0 || queued == 0 {
			produce(t, p, rng.IntN(min(room, 1000)+1), 0)
			queued++
		} else {
			mustConsume(t, c)
			queued--
		}
	}
	if wrapped == 0 || capped == 0 || tight == 0 {
		t.Fatalf("walk saw %d wrap-based, %d MaxRecord-capped and %d sub-4KiB rooms; want all three", wrapped, capped, tight)
	}
}

func TestTryReserveRunTakesTheRoomWhenAtLeastMin(t *testing.T) {
	p, c := newPair(t, newRegion(t, testCap))
	maxRec := MaxRecord(testCap)
	buf, err := p.TryReserveRun(4096)
	if err != nil || len(buf) != maxRec {
		t.Fatalf("TryReserveRun(4096) on an empty ring = %d bytes, %v; want MaxRecord %d", len(buf), err, maxRec)
	}
	p.Commit()
	for p.Room() >= 4096 {
		produce(t, p, min(p.Room(), 1000), 0)
	}
	if _, err := p.TryReserveRun(4096); !errors.Is(err, ErrFull) {
		t.Fatalf("TryReserveRun(4096) with %d bytes of room = %v; want ErrFull", p.Room(), err)
	}
	if room := p.Room(); room > 0 {
		if buf, err := p.TryReserveRun(room); err != nil || len(buf) != room {
			t.Fatalf("TryReserveRun(Room() = %d) = %d bytes, %v; want the room", room, len(buf), err)
		}
		p.Abort()
	}
	mustConsume(t, c)
	if room := p.Room(); room < 4096 {
		t.Fatalf("Room after releasing the MaxRecord record = %d; want at least 4096", room)
	} else if buf, err := p.TryReserveRun(4096); err != nil || len(buf) != room {
		t.Fatalf("TryReserveRun(4096) after a release = %d bytes, %v; want the room, %d", len(buf), err, room)
	}
}

func TestTryReserveRunRejectsMinOverMaxRecord(t *testing.T) {
	p, _ := newPair(t, newRegion(t, testCap))
	if _, err := p.TryReserveRun(MaxRecord(testCap) + 1); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("TryReserveRun(MaxRecord+1) on an empty ring = %v; want ErrTooLarge", err)
	}
}
