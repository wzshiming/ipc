//go:build !darwin

package control

import (
	"io"
	"net"
)

// steadyReader returns the reader Next takes frames from: c itself, since a
// plain read asks the kernel for no descriptors and it discards the ones a
// peer attaches.
func steadyReader(c *net.UnixConn, _ []byte) io.Reader { return c }
