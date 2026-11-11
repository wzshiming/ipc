//go:build !darwin && !linux

package control

import (
	"context"
	"net"
	"time"
)

// unbound reports whether a stream dial to the socket file at path is refused.
func unbound(ctx context.Context, path string) bool {
	d := net.Dialer{Timeout: time.Second}
	c, err := d.DialContext(ctx, "unix", path)
	if err == nil {
		c.Close()
		return false
	}
	return isConnRefused(err)
}
