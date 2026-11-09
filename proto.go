package ipc

import (
	"encoding/binary"
	"time"

	"github.com/wzshiming/ipc/internal/shm"
)

// Control protocol, version 1. The dialer sends Hello; the listener answers
// Welcome (with the segment token) or Reject (with a reason) and closes.
// Afterwards only Doorbell and Bye frames flow, in both directions.
const (
	protoVersion = 1

	frameHello    uint8 = 1
	frameWelcome  uint8 = 2
	frameReject   uint8 = 3
	frameDoorbell uint8 = 4
	frameBye      uint8 = 5

	handshakeTimeout = 5 * time.Second
)

// Ring direction carried by a Doorbell: ring 2i+dir of lane i.
const (
	dirListenerToDialer uint8 = 0
	dirDialerToListener uint8 = 1
)

// hello is version u32 | lanes u64 | ringSize u64, little-endian.
type hello struct {
	version  uint32
	lanes    uint64
	ringSize uint64
}

const helloSize = 20

func (h hello) encode() []byte {
	b := binary.LittleEndian.AppendUint32(make([]byte, 0, helloSize), h.version)
	b = binary.LittleEndian.AppendUint64(b, h.lanes)
	return binary.LittleEndian.AppendUint64(b, h.ringSize)
}

func decodeHello(p []byte) (hello, error) {
	if len(p) != helloSize {
		return hello{}, errProtocol
	}
	return hello{
		version:  binary.LittleEndian.Uint32(p),
		lanes:    binary.LittleEndian.Uint64(p[4:]),
		ringSize: binary.LittleEndian.Uint64(p[12:]),
	}, nil
}

// reject returns why the listener refuses h, or "" to accept it.
func (h hello) reject() string {
	switch {
	case h.version != protoVersion:
		return "unsupported protocol version"
	case !validLanes(h.lanes):
		return "lanes outside [1, 1024]"
	case !validRingSize(h.ringSize):
		return "ring size not a power of two in [64 KiB, 1 GiB]"
	}
	return ""
}

// welcome is lanes u64 | ringSize u64 | shm token payload.
type welcome struct {
	lanes    uint64
	ringSize uint64
	token    []byte
}

func (w welcome) encode() []byte {
	b := binary.LittleEndian.AppendUint64(make([]byte, 0, 16+len(w.token)), w.lanes)
	b = binary.LittleEndian.AppendUint64(b, w.ringSize)
	return append(b, w.token...)
}

func decodeWelcome(p []byte) (welcome, error) {
	if len(p) < 16 {
		return welcome{}, errProtocol
	}
	w := welcome{
		lanes:    binary.LittleEndian.Uint64(p),
		ringSize: binary.LittleEndian.Uint64(p[8:]),
		token:    p[16:],
	}
	if !validLanes(w.lanes) || !validRingSize(w.ringSize) {
		return welcome{}, errProtocol
	}
	return w, nil
}

// doorbell is lane u16 | dir u8.
type doorbell [3]byte

func newDoorbell(lane int, dir uint8) doorbell {
	var d doorbell
	binary.LittleEndian.PutUint16(d[:], uint16(lane))
	d[2] = dir
	return d
}

func decodeDoorbell(p []byte) (lane int, dir uint8, err error) {
	if len(p) != len(doorbell{}) || p[2] > dirDialerToListener {
		return 0, 0, errProtocol
	}
	return int(binary.LittleEndian.Uint16(p)), p[2], nil
}

// discardFDs closes descriptors received where none belong: DecodeToken owns
// its fds and closes them when the payload is invalid.
func discardFDs(fds []int) {
	if len(fds) > 0 {
		shm.DecodeToken(nil, fds)
	}
}
