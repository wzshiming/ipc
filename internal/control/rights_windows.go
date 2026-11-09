//go:build windows

package control

import (
	"io"
	"net"
)

// Windows AF_UNIX has no control messages, so the Msg variants are unusable.
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
