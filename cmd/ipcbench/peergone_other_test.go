//go:build !windows

package main

import "syscall"

var peerGoneErrnos = []error{syscall.ECONNRESET, syscall.EPIPE, syscall.ECONNABORTED, syscall.ENOTCONN}
