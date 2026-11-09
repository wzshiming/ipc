//go:build darwin || linux

package shm

import (
	"errors"

	"golang.org/x/sys/unix"
)

type segSys struct {
	fd   int
	name string // darwin: the unlinked shm_open name, for tests
}

func (s *segSys) release(mem []byte) error {
	err := unix.Munmap(mem)
	if cerr := unix.Close(s.fd); err == nil {
		err = cerr
	}
	s.fd = -1
	return err
}

// Create returns a new anonymous, zero-filled segment of size bytes.
func Create(size int) (*Segment, error) {
	if err := checkSize(size); err != nil {
		return nil, err
	}
	fd, name, err := createFD()
	if err != nil {
		return nil, err
	}
	n := mapLen(size)
	if err := unix.Ftruncate(fd, int64(n)); err != nil {
		unix.Close(fd)
		return nil, err
	}
	return mapFD(fd, name, size, n)
}

func mapFD(fd int, name string, size, n int) (*Segment, error) {
	mem, err := unix.Mmap(fd, 0, n, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
	if err != nil {
		unix.Close(fd)
		return nil, err
	}
	if len(mem) != n {
		unix.Munmap(mem)
		unix.Close(fd)
		return nil, errors.New("shm: short mapping")
	}
	return &Segment{mem: mem, size: size, sys: segSys{fd: fd, name: name}}, nil
}
