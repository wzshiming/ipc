package main

// peerGone reports whether a socket call failed because the parent closed first.
// Plan 9's syscall package has no socket errnos to match.
func peerGone(error) bool { return false }
