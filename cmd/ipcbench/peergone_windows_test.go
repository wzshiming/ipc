package main

import (
	"syscall"

	"golang.org/x/sys/windows"
)

var peerGoneErrnos = []error{
	syscall.ECONNRESET, syscall.EPIPE, syscall.ECONNABORTED, syscall.ENOTCONN,
	windows.WSAECONNRESET, windows.WSAECONNABORTED, windows.WSAENETRESET, windows.WSAESHUTDOWN,
	windows.WSAENOTCONN,
}
