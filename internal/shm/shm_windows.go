package shm

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"unsafe"

	"golang.org/x/sys/windows"
)

type segSys struct {
	h    windows.Handle
	name string
}

func (s *segSys) release(mem []byte) error {
	err := windows.UnmapViewOfFile(uintptr(unsafe.Pointer(unsafe.SliceData(mem))))
	if cerr := windows.CloseHandle(s.h); err == nil {
		err = cerr
	}
	s.h = 0
	return err
}

func randomName() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return namePrefix + hex.EncodeToString(b[:]), nil
}

// Create returns a new anonymous, zero-filled segment of size bytes.
func Create(size int) (*Segment, error) {
	if err := checkSize(size); err != nil {
		return nil, err
	}
	name, err := randomName()
	if err != nil {
		return nil, err
	}
	p, err := windows.UTF16PtrFromString(name)
	if err != nil {
		return nil, err
	}
	n := uint64(mapLen(size))
	h, err := windows.CreateFileMapping(windows.InvalidHandle, nil, windows.PAGE_READWRITE, uint32(n>>32), uint32(n), p)
	if err != nil {
		if h != 0 {
			windows.CloseHandle(h)
		}
		return nil, err
	}
	return mapHandle(h, name, size)
}

func mapHandle(h windows.Handle, name string, size int) (*Segment, error) {
	addr, err := windows.MapViewOfFile(h, windows.FILE_MAP_READ|windows.FILE_MAP_WRITE, 0, 0, 0)
	if err != nil {
		windows.CloseHandle(h)
		return nil, err
	}
	n := mapLen(size)
	var mbi windows.MemoryBasicInformation
	err = windows.VirtualQuery(addr, &mbi, unsafe.Sizeof(mbi))
	if err == nil && mbi.RegionSize != uintptr(n) {
		err = errors.New("shm: mapping size does not match token size")
	}
	if err != nil {
		windows.UnmapViewOfFile(addr)
		windows.CloseHandle(h)
		return nil, err
	}
	// addr is OS-allocated memory outside the Go heap; convert without a uintptr->Pointer cast.
	mem := unsafe.Slice(*(**byte)(unsafe.Pointer(&addr)), n)
	return &Segment{mem: mem, size: size, sys: segSys{h: h, name: name}}, nil
}

var procOpenFileMappingW = windows.NewLazySystemDLL("kernel32.dll").NewProc("OpenFileMappingW")

func openMapping(name string) (windows.Handle, error) {
	p, err := windows.UTF16PtrFromString(name)
	if err != nil {
		return 0, err
	}
	r, _, e := procOpenFileMappingW.Call(windows.FILE_MAP_READ|windows.FILE_MAP_WRITE, 0, uintptr(unsafe.Pointer(p)))
	if r == 0 {
		return 0, e
	}
	return windows.Handle(r), nil
}

// Export returns a Token carrying the segment's mapping name.
func (s *Segment) Export() (Token, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.mem == nil {
		return Token{}, ErrClosed
	}
	return Token{Size: s.size, name: s.sys.name}, nil
}

// Close is a no-op on Windows: a token holds only a name.
func (t *Token) Close() error { return nil }

func closeFDs([]int) {}

func decodeSys(size int, rest []byte, fds []int) (Token, error) {
	if len(fds) != 0 {
		return Token{}, errors.New("shm: token carries fds on Windows")
	}
	name := string(rest)
	if !validMappingName(name) {
		return Token{}, errors.New("shm: invalid mapping name")
	}
	return Token{Size: size, name: name}, nil
}

// Attach opens the peer's existing mapping by name and maps it.
func Attach(tok Token) (*Segment, error) {
	if err := checkSize(tok.Size); err != nil {
		return nil, err
	}
	h, err := openMapping(tok.name)
	if err != nil {
		return nil, err
	}
	return mapHandle(h, tok.name, tok.Size)
}
