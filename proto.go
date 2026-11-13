package ipc

import (
	"encoding/binary"
	"time"
)

// Control protocol, version 2. The dialer sends Hello; the listener answers
// Welcome (with the segment token) or Reject (with a reason) and closes.
// Afterwards only Doorbell and Bye frames flow, in both directions.
const (
	protoVersion = 2

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

// hello is version u32 | ringSize u64, little-endian. Every version starts
// with the version word; the rest belongs to that version.
type hello struct {
	version  uint32
	ringSize uint64
}

const helloSize = 12

func (h hello) encode() []byte {
	b := binary.LittleEndian.AppendUint32(make([]byte, 0, helloSize), h.version)
	return binary.LittleEndian.AppendUint64(b, h.ringSize)
}

// decodeHello reads a hello of our version; of another version it reads only
// the version word, so that reject can name it.
func decodeHello(p []byte) (hello, error) {
	if len(p) < 4 {
		return hello{}, errProtocol
	}
	h := hello{version: binary.LittleEndian.Uint32(p)}
	if h.version != protoVersion {
		return h, nil
	}
	if len(p) != helloSize {
		return hello{}, errProtocol
	}
	h.ringSize = binary.LittleEndian.Uint64(p[4:])
	return h, nil
}

// reject returns why the listener refuses h, or "" to accept it.
func (h hello) reject() string {
	switch {
	case h.version != protoVersion:
		return "unsupported protocol version"
	case !validRingSize(h.ringSize):
		return "ring size not a power of two in [64 KiB, 512 MiB]"
	}
	return ""
}

// welcome is ringSize u64 | shm token payload.
type welcome struct {
	ringSize uint64
	token    []byte
}

func (w welcome) encode() []byte {
	b := binary.LittleEndian.AppendUint64(make([]byte, 0, 8+len(w.token)), w.ringSize)
	return append(b, w.token...)
}

func decodeWelcome(p []byte) (welcome, error) {
	if len(p) < 8 {
		return welcome{}, errProtocol
	}
	w := welcome{
		ringSize: binary.LittleEndian.Uint64(p),
		token:    p[8:],
	}
	if !validRingSize(w.ringSize) {
		return welcome{}, errProtocol
	}
	return w, nil
}

// doorbell is dir u8: the ring whose parked peer it wakes.
type doorbell [1]byte

func decodeDoorbell(p []byte) (dir uint8, err error) {
	if len(p) != len(doorbell{}) || p[0] > dirDialerToListener {
		return 0, errProtocol
	}
	return p[0], nil
}
