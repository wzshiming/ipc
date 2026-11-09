// Package control carries typed frames with optional descriptor attachments
// between two processes over an AF_UNIX stream socket.
package control

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"runtime"
	"sync"
	"time"
)

// Listener accepts control links on an AF_UNIX socket path.
type Listener struct {
	l *net.UnixListener
}

// Listen listens on the AF_UNIX socket at path. A socket file left behind by
// a dead listener is removed; a live one fails with EADDRINUSE.
func Listen(path string) (*Listener, error) {
	addr := &net.UnixAddr{Name: path, Net: "unix"}
	l, err := net.ListenUnix("unix", addr)
	if err != nil && isStale(path) {
		if rerr := os.Remove(path); rerr == nil || os.IsNotExist(rerr) {
			l, err = net.ListenUnix("unix", addr)
		}
	}
	if err != nil {
		return nil, unsupportedErr(err)
	}
	if runtime.GOOS != "windows" {
		if err := os.Chmod(path, 0o600); err != nil {
			l.Close()
			return nil, err
		}
	}
	return &Listener{l: l}, nil
}

// isStale reports whether path is a socket file nobody is listening on.
func isStale(path string) bool {
	fi, err := os.Lstat(path)
	if err != nil || fi.Mode().Type() != os.ModeSocket {
		return false
	}
	c, err := net.DialTimeout("unix", path, time.Second)
	if err == nil {
		c.Close()
		return false
	}
	return isConnRefused(err)
}

// Accept waits for and returns the next link.
func (l *Listener) Accept() (*Link, error) {
	c, err := l.l.AcceptUnix()
	if err != nil {
		return nil, closedErr(err)
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
		return nil, unsupportedErr(err)
	}
	return newLink(c.(*net.UnixConn)), nil
}

// Link is one end of a control connection.
type Link struct {
	c   *net.UnixConn
	oob []byte

	wmu  sync.Mutex
	wbuf []byte

	closeOnce sync.Once
	closeErr  error
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

	k.wmu.Lock()
	defer k.wmu.Unlock()
	k.wbuf = append(append(k.wbuf[:0], h[:]...), payload...)
	rest := k.wbuf
	if len(fds) > 0 {
		if err := writeFirst(k.c, rest[:1], fds); err != nil {
			return closedErr(err)
		}
		rest = rest[1:]
	}
	_, err := k.c.Write(rest)
	return closedErr(err)
}

// Recv blocks for the next frame. It returns io.EOF once the peer has closed.
func (k *Link) Recv() (typ uint8, payload []byte, fds []int, err error) {
	var h [headerSize]byte
	fds, err = readFirst(k.c, h[:1], k.oob)
	if err != nil {
		return 0, nil, nil, closedErr(err)
	}
	defer func() {
		if err != nil {
			closeFDs(fds)
			fds = nil
			err = closedErr(err)
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

// Close closes the link and wakes a blocked Recv; later calls return nil.
func (k *Link) Close() error {
	k.closeOnce.Do(func() { k.closeErr = k.c.Close() })
	return k.closeErr
}

// SetDeadline sets the read and write deadlines.
func (k *Link) SetDeadline(t time.Time) error { return k.c.SetDeadline(t) }

// SetReadDeadline sets the deadline for Recv. A deadline that expires
// mid-frame leaves the stream misaligned; the link must then be closed.
func (k *Link) SetReadDeadline(t time.Time) error { return k.c.SetReadDeadline(t) }

// SetWriteDeadline sets the deadline for Send, with the same caveat as
// SetReadDeadline for a frame partly written.
func (k *Link) SetWriteDeadline(t time.Time) error { return k.c.SetWriteDeadline(t) }

// closedErr maps use of a closed connection to ErrClosed and passes other
// errors unwrapped so os.IsTimeout keeps working.
func closedErr(err error) error {
	if errors.Is(err, net.ErrClosed) {
		return ErrClosed
	}
	return err
}
