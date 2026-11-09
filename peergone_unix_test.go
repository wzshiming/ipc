//go:build !windows

package ipc

import "syscall"

var peerGoneErrnos = []error{syscall.ECONNRESET, syscall.EPIPE, syscall.ECONNABORTED}
