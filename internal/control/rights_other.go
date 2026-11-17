//go:build !darwin && !linux

package control

import (
	"io"
	"net"
)

// Only darwin and linux pass descriptors; Windows AF_UNIX has no control messages.
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

// CloseFDs does nothing: without descriptor passing Recv returns none.
func CloseFDs([]int) {}
