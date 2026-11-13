package ipc

import (
	"errors"
	"math/bits"
	"time"

	"github.com/wzshiming/ipc/internal/shm"
)

// Control protocol. The dialer sends Hello; the listener answers Welcome
// (with the segment token) or Reject (with a reason) and closes.
// Afterwards only Doorbell and Bye frames flow, in both directions. Hello
// opens with two magic bytes, and Hello and Welcome carry ring sizes as
// base-2 exponents.
const (
	frameHello    uint8 = 1
	frameWelcome  uint8 = 2
	frameReject   uint8 = 3
	frameDoorbell uint8 = 4
	frameBye      uint8 = 5

	handshakeTimeout = 5 * time.Second
)

// Ring direction: the ring's index in the segment and what a Doorbell carries.
const (
	dirListenerToDialer uint8 = 0
	dirDialerToListener uint8 = 1
)

// encodeRingSize is the exponent e of ring size n = 2^e, or 0, which no peer
// accepts, when n is not a power of two.
func encodeRingSize(n int) uint8 {
	if n <= 0 || n&(n-1) != 0 {
		return 0
	}
	return uint8(bits.TrailingZeros(uint(n)))
}

// decodeRingSize is the ring size 2^e, or false when e is out of range.
func decodeRingSize(e uint8) (int, bool) {
	if e < encodeRingSize(minRingSize) || e > encodeRingSize(maxRingSize) {
		return 0, false
	}
	return 1 << e, true
}

const helloMagic = "\xa5\x5a"

// hello is helloMagic | e u8: the dialer asks for rings of 2^e bytes.
type hello struct {
	ringSize int
}

const helloSize = len(helloMagic) + 1

func (h hello) encode() []byte {
	return append([]byte(helloMagic), encodeRingSize(h.ringSize))
}

// decodeHello's errors, whose text is the listener's Reject reason.
var (
	errMalformedHello = errors.New("malformed hello")
	errRingSize       = errors.New("ring size not a power of two in [64 KiB, 512 MiB]")
)

func decodeHello(p []byte) (hello, error) {
	if len(p) != helloSize || string(p[:len(helloMagic)]) != helloMagic {
		return hello{}, errMalformedHello
	}
	n, ok := decodeRingSize(p[len(helloMagic)])
	if !ok {
		return hello{}, errRingSize
	}
	return hello{ringSize: n}, nil
}

// welcome is e u8 | shm token payload: the listener grants rings of 2^e bytes.
type welcome struct {
	ringSize int
	token    []byte
}

func (w welcome) encode() []byte {
	return append([]byte{encodeRingSize(w.ringSize)}, w.token...)
}

func decodeWelcome(p []byte) (welcome, error) {
	if len(p) == 0 {
		return welcome{}, errProtocol
	}
	n, ok := decodeRingSize(p[0])
	if !ok {
		return welcome{}, errProtocol
	}
	return welcome{ringSize: n, token: p[1:]}, nil
}

// doorbell is dir u8: the ring whose parked peer it wakes.
type doorbell [1]byte

func decodeDoorbell(p []byte) (dir uint8, err error) {
	if len(p) != len(doorbell{}) || p[0] > dirDialerToListener {
		return 0, errProtocol
	}
	return p[0], nil
}

// discardFDs closes descriptors received where none belong: DecodeToken owns
// its fds and closes them when the payload is invalid.
func discardFDs(fds []int) {
	if len(fds) > 0 {
		shm.DecodeToken(nil, fds)
	}
}
