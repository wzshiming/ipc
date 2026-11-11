//go:build !plan9

package control

import "net"

// keepSocketFile stops l's Close from removing the socket file, which
// Listener.Close removes itself while it is still the one Listen made.
func keepSocketFile(l *net.UnixListener) { l.SetUnlinkOnClose(false) }
