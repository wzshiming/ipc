package ipc

import "errors"

var (
	// ErrPeerClosed is returned by sends once the peer closed the connection
	// or died, and by receives once a dead peer's records are drained.
	ErrPeerClosed = errors.New("ipc: peer closed")
	// ErrWouldBlock is returned by TryReserve on a full ring and by TryNext on an empty one.
	ErrWouldBlock = errors.New("ipc: operation would block")
	// ErrTooLarge is returned for a record longer than MaxRecord.
	ErrTooLarge = errors.New("ipc: record larger than MaxRecord")
	// ErrCorrupt reports shared memory that violates the ring format.
	ErrCorrupt = errors.New("ipc: shared memory corrupt")
	// ErrRejected is returned by Dial when the listener refuses the handshake.
	ErrRejected = errors.New("ipc: rejected by listener")

	errOrder    = errors.New("ipc: record calls out of order")
	errProtocol = errors.New("ipc: control protocol violation")
)
