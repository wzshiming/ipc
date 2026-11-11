//go:build darwin || linux

package control

import (
	"context"
	"net"
)

// unbound reports whether no socket is bound to the socket file at path. A
// datagram connect is refused only then; a stream listener answers EPROTOTYPE
// even with its accept queue full.
func unbound(ctx context.Context, path string) bool {
	var d net.Dialer
	c, err := d.DialContext(ctx, "unixgram", path)
	if err == nil {
		c.Close()
		return false
	}
	return isConnRefused(err)
}
