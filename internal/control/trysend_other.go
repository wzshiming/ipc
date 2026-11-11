//go:build !unix && !windows

package control

import "net"

// sender cannot bound a write here, where no AF_UNIX link exists.
type sender struct{}

func (*sender) init(*net.UnixConn) {}

func (*sender) close() {}

func (*sender) send([]byte) error { return ErrUnsupported }
