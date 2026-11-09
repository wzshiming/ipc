package ipc

import (
	"fmt"
	"time"

	"github.com/wzshiming/ipc/internal/ring"
	"github.com/wzshiming/ipc/internal/shm"
)

const (
	defaultLanes    = 1
	defaultRingSize = 4 << 20
	defaultSpin     = 50 * time.Microsecond

	maxLanes    = 1024
	minRingSize = 64 << 10
	maxRingSize = 1 << 30
)

// Config sizes a connection. On a Listener it sets the limits and defaults
// for accepted connections; on Dial it is a request the listener clamps to
// its limits, and both ends then use the clamped values.
type Config struct {
	// Lanes is the number of independent bidirectional lanes; 0 means 1.
	Lanes int
	// RingSize is each lane's ring capacity per direction in bytes, a power
	// of two in [64 KiB, 1 GiB]; 0 means 4 MiB.
	RingSize int
	// Spin is how long a blocking call polls before parking; 0 means 50 µs,
	// a negative value parks at once. It is local to each end.
	Spin time.Duration
}

func (c Config) withDefaults() Config {
	if c.Lanes == 0 {
		c.Lanes = defaultLanes
	}
	if c.RingSize == 0 {
		c.RingSize = defaultRingSize
	}
	switch {
	case c.Spin == 0:
		c.Spin = defaultSpin
	case c.Spin < 0:
		c.Spin = 0
	}
	return c
}

func validLanes(n uint64) bool { return n >= 1 && n <= maxLanes }

func validRingSize(n uint64) bool {
	return n >= minRingSize && n <= maxRingSize && n&(n-1) == 0
}

// segmentSize is the size of a segment holding 2×lanes rings.
func segmentSize(lanes, ringSize uint64) uint64 {
	return 2 * lanes * uint64(ring.Size(int(ringSize)))
}

func (c Config) checkListener() error {
	if c.Lanes < 0 || !validLanes(uint64(c.Lanes)) {
		return fmt.Errorf("ipc: Lanes %d outside [1, %d]", c.Lanes, maxLanes)
	}
	if c.RingSize < 0 || !validRingSize(uint64(c.RingSize)) {
		return fmt.Errorf("ipc: RingSize %d is not a power of two in [64 KiB, 1 GiB]", c.RingSize)
	}
	if segmentSize(uint64(c.Lanes), uint64(c.RingSize)) > shm.MaxSize {
		return fmt.Errorf("ipc: %d lanes of %d-byte rings exceed the %d-byte segment limit", c.Lanes, c.RingSize, shm.MaxSize)
	}
	return nil
}
