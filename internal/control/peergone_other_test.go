//go:build !windows

package control

import "syscall"

var peerGoneErrnos = []error{syscall.ECONNRESET, syscall.EPIPE, syscall.ECONNABORTED}
