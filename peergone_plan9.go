package ipc

import (
	"errors"
	"io"
)

// isPeerGone reports whether a link error means the peer closed or crashed.
// Plan 9's syscall package has no socket errnos to match.
func isPeerGone(err error) bool {
	return errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF)
}
