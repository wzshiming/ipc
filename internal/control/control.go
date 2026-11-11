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
	"strings"
	"sync"
	"time"
)

// Listener accepts control links on an AF_UNIX socket path.
type Listener struct {
	l *net.UnixListener

	mu   sync.Mutex
	path string
	file os.FileInfo // the socket file Listen made at path, until Close
	lock *pathLock   // held until Close; nil where none is taken
}

// Listen listens on the AF_UNIX socket at path. On darwin, linux and windows
// it holds the lock file path+".lock" until Close, so that one Listener at a
// time owns path. A socket file left behind by a dead listener is replaced;
// another Listener or a live server on path fails with EADDRINUSE. On Linux a
// path starting with @ is an abstract name, which has no file. ctx bounds
// the setup, including the probe of an existing socket file.
func Listen(ctx context.Context, path string) (*Listener, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	addr := &net.UnixAddr{Name: path, Net: "unix"}
	if runtime.GOOS == "linux" && strings.HasPrefix(path, "@") {
		l, err := net.ListenUnix("unix", addr)
		if err != nil {
			return nil, err
		}
		return &Listener{l: l}, nil
	}
	lock, err := lockPath(path)
	if err != nil {
		return nil, &net.OpError{Op: "listen", Net: "unix", Addr: addr, Err: err}
	}
	l, file, err := bind(ctx, path, addr)
	if err != nil {
		lock.unlock()
		return nil, err
	}
	return &Listener{l: l, path: path, file: file, lock: lock}, nil
}

// bind listens on path, replacing a socket file nobody listens on, and
// returns the socket file it made. The caller holds the path's lock.
func bind(ctx context.Context, path string, addr *net.UnixAddr) (*net.UnixListener, os.FileInfo, error) {
	l, err := net.ListenUnix("unix", addr)
	if err != nil && isStale(ctx, path) {
		if rerr := os.Remove(path); rerr == nil || os.IsNotExist(rerr) {
			l, err = net.ListenUnix("unix", addr)
		}
	}
	if err != nil {
		if cerr := ctx.Err(); cerr != nil {
			return nil, nil, cerr
		}
		return nil, nil, unsupportedErr(err)
	}
	if runtime.GOOS != "windows" {
		err = os.Chmod(path, 0o600)
	}
	var file os.FileInfo
	if err == nil {
		file, err = os.Lstat(path)
	}
	if err != nil {
		l.Close()
		return nil, nil, err
	}
	keepSocketFile(l)
	return l, file, nil
}

// isStale reports whether path is a socket file nobody is listening on.
func isStale(ctx context.Context, path string) bool {
	fi, err := os.Lstat(path)
	if err != nil || fi.Mode().Type() != os.ModeSocket {
		return false
	}
	d := net.Dialer{Timeout: time.Second}
	c, err := d.DialContext(ctx, "unix", path)
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

// Close stops listening and removes the socket file if it is still the one
// Listen made, then gives path up; the lock file goes too on darwin and
// linux, and stays on windows.
func (l *Listener) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.file != nil {
		// Checked while the socket is open, which keeps its file's inode from reuse.
		if fi, err := os.Lstat(l.path); err == nil && os.SameFile(fi, l.file) {
			os.Remove(l.path)
		}
		l.file = nil
	}
	err := l.l.Close()
	l.lock.unlock()
	l.lock = nil
	return err
}

// Addr returns the listener's address.
func (l *Listener) Addr() net.Addr { return l.l.Addr() }

// SetDeadline sets the deadline for Accept; a zero value disables it.
func (l *Listener) SetDeadline(t time.Time) error { return closedErr(l.l.SetDeadline(t)) }

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
	tx   sender // TrySend's writer, used under wmu

	closeOnce sync.Once
	closeErr  error
}

func newLink(c *net.UnixConn) *Link {
	k := &Link{c: c, oob: make([]byte, oobSize)}
	k.tx.init(c)
	return k
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

// trySendWait bounds the time TrySend waits for socket buffer space.
const trySendWait = 50 * time.Millisecond

// TrySend writes one frame without descriptors, waiting at most 50 ms in all
// for socket buffer space. ErrBusy means the socket did not take the whole
// frame, part of which may have gone out: the link is then unusable.
func (k *Link) TrySend(typ uint8, payload []byte) error {
	if len(payload) > MaxPayload {
		return ErrPayloadTooLarge
	}
	var h [headerSize]byte
	h[0] = typ
	putLength(h[1:], len(payload))

	k.wmu.Lock()
	defer k.wmu.Unlock()
	k.wbuf = append(append(k.wbuf[:0], h[:]...), payload...)
	return closedErr(k.tx.send(k.wbuf))
}

// Recv blocks for the next frame. It returns io.EOF once the peer has closed.
// The descriptors that came with the frame's first byte, at most MaxFDs, are
// the caller's to close; those that came with any other byte are closed.
func (k *Link) Recv() (typ uint8, payload []byte, fds []int, err error) {
	var h [headerSize]byte
	fds, err = k.readFirst(h[:1])
	if err != nil {
		return 0, nil, nil, closedErr(err)
	}
	defer func() {
		if err != nil {
			CloseFDs(fds)
			fds = nil
			err = closedErr(err)
		}
	}()
	if err = k.readRest(h[1:]); err != nil {
		return 0, nil, fds, err
	}
	n, err := length(h[1:])
	if err != nil {
		return 0, nil, fds, err
	}
	payload = make([]byte, n)
	if err = k.readRest(payload); err != nil {
		return 0, nil, fds, err
	}
	return h[0], payload, fds, nil
}

// readFirst reads a frame's first byte and the descriptors that came with it.
func (k *Link) readFirst(b []byte) ([]int, error) {
	n, fds, err := readMsg(k.c, b, k.oob)
	if err == nil && n == 0 {
		err = io.EOF
	}
	if err == nil && len(fds) > MaxFDs {
		err = ErrTooManyFDs
	}
	if err != nil {
		CloseFDs(fds)
		return nil, err
	}
	return fds, nil
}

// readRest fills b as io.ReadFull does and closes the descriptors that come
// with its bytes.
func (k *Link) readRest(b []byte) error {
	for i := 0; i < len(b); {
		n, fds, err := readMsg(k.c, b[i:], k.oob)
		CloseFDs(fds)
		if err == io.EOF && i > 0 {
			err = io.ErrUnexpectedEOF
		}
		if err != nil {
			return err
		}
		i += n
	}
	return nil
}

// CloseWrite shuts the link's sending side: after the frames already sent the
// peer reads io.EOF, while Recv goes on.
func (k *Link) CloseWrite() error { return closedErr(k.c.CloseWrite()) }

// Close closes the link and wakes a blocked Recv; later calls return nil.
func (k *Link) Close() error {
	k.closeOnce.Do(func() {
		k.closeErr = k.c.Close()
		k.tx.close()
	})
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
