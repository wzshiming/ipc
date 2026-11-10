package ipc

import "errors"

var (
	// ErrPeerClosed reports that the peer closed the connection or died.
	ErrPeerClosed = errors.New("ipc: peer closed")
	// ErrWouldBlock is returned by TryReserve on a full lane and by TryNext on an empty one.
	ErrWouldBlock = errors.New("ipc: operation would block")
	// ErrTooLarge is returned for a record longer than MaxRecord.
	ErrTooLarge = errors.New("ipc: record larger than MaxRecord")
	// ErrCorrupt reports shared memory that violates the ring format.
	ErrCorrupt = errors.New("ipc: shared memory corrupt")
	// ErrRejected is returned by Dial when the listener refuses the handshake.
	ErrRejected = errors.New("ipc: rejected by listener")

	errOrder    = errors.New("ipc: lane calls out of order")
	errProtocol = errors.New("ipc: control protocol violation")
)
