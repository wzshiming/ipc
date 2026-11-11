//go:build !windows

package main

import (
	"errors"
	"syscall"
)

// peerGone reports whether a socket call failed because the parent closed first.
func peerGone(err error) bool {
	return errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.EPIPE) ||
		errors.Is(err, syscall.ECONNABORTED) || errors.Is(err, syscall.ENOTCONN)
}
