//go:build darwin

package control

import (
	"io"
	"net"
)

// steadyReader returns the reader Next takes frames from. The darwin kernel
// installs the descriptors of an SCM_RIGHTS message in the receiver even on
// a plain read, so the reader asks for them into oob and closes them.
func steadyReader(c *net.UnixConn, oob []byte) io.Reader {
	return &rightsCloser{c: c, oob: oob}
}

type rightsCloser struct {
	c   *net.UnixConn
	oob []byte
}

func (r *rightsCloser) Read(p []byte) (int, error) {
	n, oobn, _, _, err := r.c.ReadMsgUnix(p, r.oob)
	if oobn > 0 {
		fds, _ := parseRights(r.oob[:oobn])
		closeFDs(fds)
	}
	if n < 0 { // the syscall's -1, which ReadMsgUnix passes through on error
		n = 0
	}
	return n, err
}
