//go:build windows

package control

import (
	"errors"
	"fmt"
	"io"
	"net"

	"golang.org/x/sys/windows"
)

// Windows AF_UNIX has no control messages, so the Msg variants are unusable.
const fdPassing = false

const oobSize = 0

func writeFirst(c *net.UnixConn, b []byte, _ []int) error {
	_, err := c.Write(b)
	return err
}

func readFirst(c *net.UnixConn, b, _ []byte) ([]int, error) {
	_, err := io.ReadFull(c, b)
	return nil, err
}

// CloseFDs does nothing: no descriptor arrives here.
func CloseFDs([]int) {}

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
