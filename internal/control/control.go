// Package control carries typed frames with optional descriptor attachments
// between two processes over an AF_UNIX stream socket.
package control

import (
	"context"
	"io"
	"net"
)

// Listener accepts control links on an AF_UNIX socket path.
type Listener struct {
	l *net.UnixListener
}

// Listen listens on the AF_UNIX socket at path.
func Listen(path string) (*Listener, error) {
	l, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		return nil, err
	}
	return &Listener{l: l}, nil
}

// Accept waits for and returns the next link.
func (l *Listener) Accept() (*Link, error) {
	c, err := l.l.AcceptUnix()
	if err != nil {
		return nil, err
	}
	return newLink(c), nil
}

// Close stops listening and removes the socket file.
func (l *Listener) Close() error { return l.l.Close() }

// Addr returns the listener's address.
func (l *Listener) Addr() net.Addr { return l.l.Addr() }

// Dial connects to the AF_UNIX socket at path.
func Dial(ctx context.Context, path string) (*Link, error) {
	var d net.Dialer
	c, err := d.DialContext(ctx, "unix", path)
	if err != nil {
		return nil, err
	}
	return newLink(c.(*net.UnixConn)), nil
}

// Link is one end of a control connection.
type Link struct {
	c   *net.UnixConn
	oob []byte
}

func newLink(c *net.UnixConn) *Link {
	return &Link{c: c, oob: make([]byte, oobSize)}
}

// Send writes one frame; fds travel with its first byte.
func (k *Link) Send(typ uint8, payload []byte, fds []int) error {
	if len(payload) > MaxPayload {
		return ErrPayloadTooLarge
	}
	if len(fds) > MaxFDs {
		return ErrTooManyFDs
	}
	if len(fds) > 0 && !fdPassing {
		return ErrUnsupported
	}
	var h [headerSize]byte
	h[0] = typ
	putLength(h[1:], len(payload))
	if err := writeFirst(k.c, h[:1], fds); err != nil {
		return err
	}
	if _, err := k.c.Write(h[1:]); err != nil {
		return err
	}
	_, err := k.c.Write(payload)
	return err
}

// Recv blocks for the next frame. It returns io.EOF once the peer has closed.
func (k *Link) Recv() (typ uint8, payload []byte, fds []int, err error) {
	var h [headerSize]byte
	fds, err = readFirst(k.c, h[:1], k.oob)
	if err != nil {
		return 0, nil, nil, err
	}
	defer func() {
		if err != nil {
			closeFDs(fds)
			fds = nil
		}
	}()
	if _, err = io.ReadFull(k.c, h[1:]); err != nil {
		return 0, nil, fds, err
	}
	n, err := length(h[1:])
	if err != nil {
		return 0, nil, fds, err
	}
	payload = make([]byte, n)
	if _, err = io.ReadFull(k.c, payload); err != nil {
		return 0, nil, fds, err
	}
	return h[0], payload, fds, nil
}

// Close closes the link.
func (k *Link) Close() error { return k.c.Close() }
