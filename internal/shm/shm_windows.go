package shm

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"unsafe"

	"golang.org/x/sys/windows"
)

const namePrefix = `Local\ipc-`

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
