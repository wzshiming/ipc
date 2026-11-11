package control

import "net"

// keepSocketFile has nothing to do: Plan 9 has no AF_UNIX listener.
func keepSocketFile(*net.UnixListener) {}
