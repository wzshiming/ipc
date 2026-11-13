// Package shm creates, maps, exports, attaches and releases anonymous
// shared-memory segments with one contract on Linux, macOS and Windows.
//
// A segment has no name in any file system or namespace once Create returns
// (Linux memfd, macOS shm_open+shm_unlink) or once the last handle is closed
// (Windows Local\ named section), so a crashed process leaves nothing behind.
package shm

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
)

const (
	// MinSize is the smallest segment accepted: one ring header.
	MinSize = 4096
	// MaxSize is the largest segment accepted: 1 GiB of data plus two header pages.
	MaxSize = 1<<30 + 2*4096

	// namePrefix starts every Windows mapping name; 32 lowercase hex chars follow.
	namePrefix = `Local\ipc-`
)

// ErrUnsupported is returned on platforms without a shared-memory backend.
var ErrUnsupported = fmt.Errorf("shm: %w", errors.ErrUnsupported)

// ErrClosed is returned when exporting a closed segment.
var ErrClosed = errors.New("shm: segment closed")

var errSize = errors.New("shm: size out of range")

// Segment is one mapped shared-memory segment.
type Segment struct {
	mu   sync.Mutex
	mem  []byte // the whole page-rounded mapping; nil once closed
	size int
	sys  segSys
}

// Bytes returns the mapped memory, len == Size(), page-aligned base.
// The slice is invalid after Close; Bytes returns nil once closed.
func (s *Segment) Bytes() []byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.mem == nil {
		return nil
	}
	return s.mem[:s.size:s.size]
}

// Size returns the size requested at Create or carried by the Token at Attach.
func (s *Segment) Size() int { return s.size }

// Close unmaps the segment and closes its handle. It is idempotent.
func (s *Segment) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.mem == nil {
		return nil
	}
	err := s.sys.release(s.mem)
	s.mem = nil
	return err
}

func checkSize(size int) error {
	if size < MinSize || size > MaxSize {
		return errSize
	}
	return nil
}

func validMappingName(name string) bool {
	hexPart, ok := strings.CutPrefix(name, namePrefix)
	if !ok || len(hexPart) != 32 {
		return false
	}
	for _, c := range []byte(hexPart) {
		if !('0' <= c && c <= '9' || 'a' <= c && c <= 'f') {
			return false
		}
	}
	return true
}

func mapLen(size int) int {
	pg := os.Getpagesize()
	return (size + pg - 1) &^ (pg - 1)
}

// Token is what a peer needs to Attach a segment: its Size plus an owned fd
// (POSIX) or the mapping name (Windows).
type Token struct {
	Size  int
	fd    int
	hasFD bool
	name  string
}

// Encode returns the token as bytes plus the fds to send alongside them
// (SCM_RIGHTS). The token keeps owning its fd.
func (t Token) Encode() (payload []byte, fds []int) {
	payload = binary.LittleEndian.AppendUint32(nil, uint32(t.Size))
	payload = append(payload, t.name...)
	if t.hasFD {
		fds = []int{t.fd}
	}
	return payload, fds
}

// DecodeToken rebuilds a Token received from a peer. It takes ownership of
// fds: they end up in the Token, or are closed when decoding fails.
func DecodeToken(payload []byte, fds []int) (Token, error) {
	if len(payload) < 4 {
		closeFDs(fds)
		return Token{}, errors.New("shm: short token payload")
	}
	size := binary.LittleEndian.Uint32(payload)
	if size < MinSize || size > MaxSize {
		closeFDs(fds)
		return Token{}, errSize
	}
	return decodeSys(int(size), payload[4:], fds)
}
