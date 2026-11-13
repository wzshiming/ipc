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
	if err := sealSize(fd); err != nil {
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

// Export returns a Token holding a new CLOEXEC dup of the segment's fd,
// owned by the caller.
func (s *Segment) Export() (Token, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.mem == nil {
		return Token{}, ErrClosed
	}
	fd, err := unix.FcntlInt(uintptr(s.sys.fd), unix.F_DUPFD_CLOEXEC, 0)
	if err != nil {
		return Token{}, err
	}
	return Token{Size: s.size, fd: fd, hasFD: true}, nil
}

// Close closes the fd the token still holds. It is idempotent.
func (t *Token) Close() error {
	if !t.hasFD {
		return nil
	}
	t.hasFD = false
	return unix.Close(t.fd)
}

func closeFDs(fds []int) {
	for _, fd := range fds {
		unix.Close(fd)
	}
}

func decodeSys(size int, rest []byte, fds []int) (Token, error) {
	if len(fds) != 1 || len(rest) != 0 {
		closeFDs(fds)
		return Token{}, errors.New("shm: token needs exactly one fd and no name")
	}
	return Token{Size: size, fd: fds[0], hasFD: true}, nil
}

// Attach maps the peer's segment. It takes ownership of tok's fd, closing it
// on failure and with the returned Segment otherwise. It refuses a segment
// whose size could still change.
func Attach(tok Token) (*Segment, error) {
	if !tok.hasFD {
		return nil, errors.New("shm: token holds no fd")
	}
	fd := tok.fd
	if err := checkSize(tok.Size); err != nil {
		unix.Close(fd)
		return nil, err
	}
	// Seals first, so the size fstat reads below is final.
	if err := checkSizeSealed(fd); err != nil {
		unix.Close(fd)
		return nil, err
	}
	n := mapLen(tok.Size)
	var st unix.Stat_t
	if err := unix.Fstat(fd, &st); err != nil {
		unix.Close(fd)
		return nil, err
	}
	if st.Size != int64(n) {
		unix.Close(fd)
		return nil, errors.New("shm: segment size does not match token size")
	}
	unix.CloseOnExec(fd)
	return mapFD(fd, "", tok.Size, n)
}
