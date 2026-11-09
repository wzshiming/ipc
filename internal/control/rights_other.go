//go:build !darwin && !linux && !windows

package control

import (
	"errors"
	"io"
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

func readFirst(c *net.UnixConn, b, _ []byte) ([]int, error) {
	_, err := io.ReadFull(c, b)
	return nil, err
}

func closeFDs([]int) {}

func isConnRefused(err error) bool { return errors.Is(err, syscall.ECONNREFUSED) }

func unsupportedErr(err error) error { return err }
