//go:build !windows

package control

import (
	"errors"
	"syscall"
)

func isConnRefused(err error) bool { return errors.Is(err, syscall.ECONNREFUSED) }

func unsupportedErr(err error) error { return err }

// PeerGone reports whether a socket call failed because the peer closed or reset.
func PeerGone(err error) bool {
	return errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.EPIPE) ||
		errors.Is(err, syscall.ECONNABORTED)
}
