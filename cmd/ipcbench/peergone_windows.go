//go:build windows

package main

import (
	"errors"
	"syscall"

	"golang.org/x/sys/windows"
)

// peerGone reports whether a socket call failed because the parent closed first.
// Winsock reports WSA* codes, which do not match syscall's invented Errnos.
func peerGone(err error) bool {
	return errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.EPIPE) ||
		errors.Is(err, syscall.ECONNABORTED) || errors.Is(err, syscall.ENOTCONN) ||
		errors.Is(err, windows.WSAECONNRESET) || errors.Is(err, windows.WSAECONNABORTED) ||
		errors.Is(err, windows.WSAENETRESET) || errors.Is(err, windows.WSAESHUTDOWN) ||
		errors.Is(err, windows.WSAENOTCONN)
}
