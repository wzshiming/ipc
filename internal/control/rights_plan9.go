package control

import (
	"io"
	"net"
)

// Plan 9 has no AF_UNIX sockets, so Listen and Dial fail before these run.
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

// CloseFDs does nothing: no descriptor arrives here.
func CloseFDs([]int) {}

// Plan 9's syscall package has no ECONNREFUSED to match.
func isConnRefused(error) bool { return false }

func unsupportedErr(err error) error { return err }
