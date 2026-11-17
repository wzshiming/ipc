//go:build windows

package control

import (
	"errors"
	"fmt"
	"syscall"

	"golang.org/x/sys/windows"
)

func isConnRefused(err error) bool { return errors.Is(err, windows.WSAECONNREFUSED) }

// unsupportedErr marks the failure of Windows before 10 1803 to create AF_UNIX sockets.
func unsupportedErr(err error) error {
	if errors.Is(err, windows.WSAEAFNOSUPPORT) ||
		errors.Is(err, windows.WSAEPROTONOSUPPORT) ||
		errors.Is(err, windows.WSAESOCKTNOSUPPORT) {
		return fmt.Errorf("%w: %w", ErrUnsupported, err)
	}
	return err
}

// PeerGone reports whether a socket call failed because the peer closed or
// reset the connection; Winsock reports WSA* codes, not syscall's invented Errnos.
func PeerGone(err error) bool {
	return errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.EPIPE) ||
		errors.Is(err, syscall.ECONNABORTED) ||
		errors.Is(err, windows.WSAECONNRESET) || errors.Is(err, windows.WSAECONNABORTED) ||
		errors.Is(err, windows.WSAENETRESET) || errors.Is(err, windows.WSAESHUTDOWN)
}
