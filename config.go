package ipc

import (
	"runtime"
	"time"

	"github.com/wzshiming/ipc/internal/ring"
	"github.com/wzshiming/ipc/internal/shm"
)

const (
	defaultRingSize = 4 << 20
	defaultSpin     = 50 * time.Microsecond

	minRingSize = 64 << 10
	// maxRingSize is the largest ring of which two fit one segment.
	maxRingSize = (shm.MaxSize - 2*ring.HeaderSize) / 2
)

func validRingSize(n uint64) bool {
	return n >= minRingSize && n <= maxRingSize && n&(n-1) == 0
}

// segmentSize is the size of a segment holding a connection's two rings.
func segmentSize(ringSize uint64) uint64 {
	return 2 * uint64(ring.Size(int(ringSize)))
}

// numCPU is runtime.NumCPU, replaced in tests.
var numCPU = runtime.NumCPU

// spinDuration maps a Spin option to how long blocking calls spin: 0 means
// the default, or none on a single CPU where spinning only delays the peer,
// and a negative value none.
func spinDuration(spin time.Duration) time.Duration {
	switch {
	case spin == 0 && numCPU() > 1:
		return defaultSpin
	case spin <= 0:
		return 0
	}
	return spin
}
