//go:build windows

package control

import (
	"syscall"

	"golang.org/x/sys/windows"
)

var peerGoneErrnos = []error{
	syscall.ECONNRESET, syscall.EPIPE, syscall.ECONNABORTED,
	windows.WSAECONNRESET, windows.WSAECONNABORTED, windows.WSAENETRESET, windows.WSAESHUTDOWN,
}
