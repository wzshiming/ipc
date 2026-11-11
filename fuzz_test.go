package ipc

import (
	"bytes"
	"encoding/binary"
	"testing"
)

// FuzzDecodeHello checks that decodeHello never panics and that what it
// decodes encodes back to its bytes: all of them for this version, the version
// word for another.
func FuzzDecodeHello(f *testing.F) {
	// Version 1's hello: version u32, then two u64 fields.
	v1 := binary.LittleEndian.AppendUint32(nil, 1)
	v1 = binary.LittleEndian.AppendUint64(v1, 1)
	v1 = binary.LittleEndian.AppendUint64(v1, 4<<20)
	f.Add(hello{version: protoVersion, ringSize: defaultRingSize}.encode())
	f.Add(v1)
	f.Add([]byte{protoVersion, 0, 0}) // a short hello
	f.Add(hello{version: protoVersion, ringSize: defaultRingSize}.encode()[:helloSize-1])
	f.Add(hello{version: protoVersion, ringSize: 1 << 30}.encode()) // an oversize ring size
	f.Fuzz(func(t *testing.T, p []byte) {
		h, err := decodeHello(p)
		if err != nil {
			return
		}
		got, want := h.encode(), p
		if h.version != protoVersion {
			got, want = got[:4], p[:4]
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("decodeHello(%x) = %+v, which encodes to %x", p, h, h.encode())
		}
	})
}

// FuzzDecodeWelcome checks that decodeWelcome never panics and that what it
// decodes encodes back to its bytes.
func FuzzDecodeWelcome(f *testing.F) {
	f.Add(welcome{ringSize: minRingSize, token: []byte("token")}.encode())
	f.Add(welcome{ringSize: 1 << 30, token: []byte("token")}.encode()) // an oversize ring size
	f.Add(welcome{ringSize: minRingSize + 8}.encode())                 // a ring size no power of two
	f.Add([]byte{0, 0, 1})                                             // a short welcome
	f.Fuzz(func(t *testing.T, p []byte) {
		w, err := decodeWelcome(p)
		if err != nil {
			return
		}
		if got := w.encode(); !bytes.Equal(got, p) {
			t.Fatalf("decodeWelcome(%x) = %+v, which encodes to %x", p, w, got)
		}
	})
}

// FuzzDecodeDoorbell checks that decodeDoorbell never panics and that the
// doorbell of the direction it decodes is its bytes.
func FuzzDecodeDoorbell(f *testing.F) {
	f.Add([]byte{dirListenerToDialer})
	f.Add([]byte{dirDialerToListener})
	f.Add([]byte{dirDialerToListener + 1})
	f.Add([]byte(nil))
	f.Add([]byte{dirListenerToDialer, 0})
	f.Fuzz(func(t *testing.T, p []byte) {
		dir, err := decodeDoorbell(p)
		if err != nil {
			return
		}
		if bell := (doorbell{dir}); !bytes.Equal(bell[:], p) {
			t.Fatalf("decodeDoorbell(%x) = %d, whose doorbell is %x", p, dir, bell)
		}
	})
}
