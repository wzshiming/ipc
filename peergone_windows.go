package ipc

import (
	"errors"
	"io"
	"syscall"

	"golang.org/x/sys/windows"
)

// isPeerGone reports whether a link error means the peer closed or crashed.
// Winsock reports WSA* codes, which do not match syscall's invented Errnos.
func isPeerGone(err error) bool {
	return errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.EPIPE) ||
		errors.Is(err, syscall.ECONNABORTED) ||
		errors.Is(err, windows.WSAECONNRESET) || errors.Is(err, windows.WSAECONNABORTED) ||
		errors.Is(err, windows.WSAENETRESET) || errors.Is(err, windows.WSAESHUTDOWN)
}
