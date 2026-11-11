package control

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// Wire format: typ u8 | len u32 little-endian | payload.
const (
	headerSize = 5

	// MaxPayload is the largest payload one frame may carry.
	MaxPayload = 64 << 10
	// MaxFDs is the largest number of descriptors one frame may carry.
	MaxFDs = 16
)

var (
	// ErrClosed is returned by operations on a closed Link or Listener.
	ErrClosed = errors.New("control: closed")
	// ErrUnsupported is returned when the platform cannot provide a feature.
	ErrUnsupported = fmt.Errorf("control: %w", errors.ErrUnsupported)
	// ErrPayloadTooLarge is returned for a payload above MaxPayload.
	ErrPayloadTooLarge = errors.New("control: payload exceeds 64 KiB")
	// ErrTooManyFDs is returned for more than MaxFDs descriptors.
	ErrTooManyFDs = errors.New("control: more than 16 fds")
	// ErrBusy is returned by TrySend when the socket did not take the frame.
	ErrBusy = errors.New("control: socket did not take the frame in time")
)

func putLength(b []byte, n int) {
	binary.LittleEndian.PutUint32(b, uint32(n))
}

func length(b []byte) (int, error) {
	n := binary.LittleEndian.Uint32(b)
	if n > MaxPayload {
		return 0, ErrPayloadTooLarge
	}
	return int(n), nil
}
