//go:build !darwin && !linux && !windows

package shm

type segSys struct{}

func (*segSys) release([]byte) error { return nil }

// Create returns ErrUnsupported on this platform.
func Create(size int) (*Segment, error) { return nil, ErrUnsupported }

// Export returns ErrUnsupported on this platform.
func (s *Segment) Export() (Token, error) { return Token{}, ErrUnsupported }

// Close is a no-op on this platform.
func (t *Token) Close() error { return nil }

func closeFDs([]int) {}

func decodeSys(int, []byte, []int) (Token, error) { return Token{}, ErrUnsupported }

// Attach returns ErrUnsupported on this platform.
func Attach(tok Token) (*Segment, error) { return nil, ErrUnsupported }
