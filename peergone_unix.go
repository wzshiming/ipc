//go:build !windows && !plan9

package ipc

import (
	"errors"
	"io"
	"syscall"
)

// isPeerGone reports whether a link error means the peer closed or crashed.
func isPeerGone(err error) bool {
	return errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.EPIPE) ||
		errors.Is(err, syscall.ECONNABORTED) || errors.Is(err, syscall.ENOTCONN)
}
