//go:build !darwin && !linux && !windows && !plan9

package control

import (
	"errors"
	"net"
	"syscall"
)

// Descriptor passing is only wired up for darwin and linux.
const fdPassing = false

const oobSize = 0

func writeFirst(c *net.UnixConn, b []byte, _ []int) error {
	_, err := c.Write(b)
	return err
}

func readMsg(c *net.UnixConn, b, _ []byte) (int, []int, error) {
	n, err := c.Read(b)
	return n, nil, err
}

// CloseFDs does nothing: no descriptor arrives here.
func CloseFDs([]int) {}

func isConnRefused(err error) bool { return errors.Is(err, syscall.ECONNREFUSED) }

func unsupportedErr(err error) error { return err }
