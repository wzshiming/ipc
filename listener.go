package ipc

import (
	"context"
	"errors"
	"net"
	"os"
	"sync"
	"time"

	"github.com/wzshiming/ipc/internal/control"
	"github.com/wzshiming/ipc/internal/ring"
	"github.com/wzshiming/ipc/internal/shm"
)

// ListenConfig contains options for listening. Its values are the limits and
// the defaults for accepted connections: a dialer's request is clamped to
// them and both ends use the clamped values.
type ListenConfig struct {
	// Lanes is the most lanes a connection may have; 0 means 1.
	Lanes int
	// RingSize is the largest ring capacity per lane direction in bytes, a
	// power of two in [64 KiB, 1 GiB]; 0 means 4 MiB.
	RingSize int
	// Spin is how long a blocking call on an accepted connection busy
	// re-checks its lane, without yielding its thread, before parking; 0
	// means 50 µs and a negative value parks at once. Each spinning call
	// holds a CPU, so choose Spin small or negative when CPUs are scarce.
	Spin time.Duration
}

// Listener accepts connections on an AF_UNIX socket path.
type Listener struct {
	cl   *control.Listener
	cfg  config
	addr addr

	mu       sync.Mutex
	deadline time.Time

	closeOnce sync.Once
	closeErr  error
}

// Listen listens on the AF_UNIX socket at path with the default
// ListenConfig. A socket file left behind by a dead listener is replaced.
func Listen(path string) (*Listener, error) {
	var lc *ListenConfig
	return lc.Listen(context.Background(), path)
}

// Listen listens on the AF_UNIX socket at path. It fails if lc's values are
// out of range or another listener is live on path; a socket file left
// behind by a dead listener is replaced. ctx bounds the setup, including the
// probe of an existing socket file, but not the returned Listener. A nil lc
// uses the defaults.
func (lc *ListenConfig) Listen(ctx context.Context, path string) (*Listener, error) {
	var cfg config
	if lc != nil {
		cfg = config(*lc)
	}
	cfg = cfg.withDefaults()
	if err := cfg.checkListener(); err != nil {
		return nil, err
	}
	cl, err := control.Listen(ctx, path)
	if err != nil {
		return nil, err
	}
	return &Listener{cl: cl, cfg: cfg, addr: addr(path)}, nil
}

// Accept waits for a dialer, completes its handshake and returns the
// connection. Dialers that fail the handshake are dropped and Accept keeps
// waiting. Past the deadline set by SetDeadline it returns
// os.ErrDeadlineExceeded, and after Close net.ErrClosed.
func (l *Listener) Accept() (*Conn, error) {
	for {
		link, err := l.cl.Accept()
		if err != nil {
			return nil, listenerErr(err)
		}
		c, local, err := l.handshake(link)
		if err == nil {
			return c, nil
		}
		if local {
			return nil, err
		}
	}
}

// listenerErr maps the control listener's close and timeout errors to the
// standard ones.
func listenerErr(err error) error {
	switch {
	case errors.Is(err, control.ErrClosed):
		return net.ErrClosed
	case errors.Is(err, os.ErrDeadlineExceeded):
		return os.ErrDeadlineExceeded
	}
	return err
}

// SetDeadline sets the deadline for Accept, including a call already
// blocked, and for the handshakes it performs; a zero t means no deadline.
func (l *Listener) SetDeadline(t time.Time) error {
	l.mu.Lock()
	l.deadline = t
	l.mu.Unlock()
	if err := l.cl.SetDeadline(t); err != nil {
		return listenerErr(err)
	}
	return nil
}

// handshakeDeadline is when a handshake begun now must have finished.
func (l *Listener) handshakeDeadline() time.Time {
	t := time.Now().Add(handshakeTimeout)
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.deadline.IsZero() && l.deadline.Before(t) {
		return l.deadline
	}
	return t
}

// handshake answers the dialer's Hello. local reports a failure on this end
// rather than the dialer's. The link is closed on failure.
func (l *Listener) handshake(link *control.Link) (c *Conn, local bool, err error) {
	var seg *shm.Segment
	defer func() {
		if err != nil {
			link.Close()
			if seg != nil {
				seg.Close()
			}
		}
	}()
	link.SetDeadline(l.handshakeDeadline())
	typ, p, fds, err := link.Recv()
	if err != nil {
		return nil, false, err
	}
	discardFDs(fds)
	if typ != frameHello || len(fds) > 0 {
		return nil, false, errProtocol
	}
	h, err := decodeHello(p)
	if err != nil {
		link.Send(frameReject, []byte("malformed hello"), nil)
		return nil, false, err
	}
	if reason := h.reject(); reason != "" {
		link.Send(frameReject, []byte(reason), nil)
		return nil, false, ErrRejected
	}
	lanes := min(h.lanes, uint64(l.cfg.Lanes))
	ringSize := min(h.ringSize, uint64(l.cfg.RingSize))
	seg, err = shm.Create(int(segmentSize(lanes, ringSize)))
	if err != nil {
		link.Send(frameReject, []byte("cannot create segment"), nil)
		return nil, true, err
	}
	mem, rs := seg.Bytes(), ring.Size(int(ringSize))
	for k := range 2 * int(lanes) {
		if err := ring.Format(mem[k*rs:(k+1)*rs], int(ringSize)); err != nil {
			return nil, true, err
		}
	}
	tok, err := seg.Export()
	if err != nil {
		return nil, true, err
	}
	payload, tokFDs := tok.Encode()
	err = link.Send(frameWelcome, welcome{lanes: lanes, ringSize: ringSize, token: payload}.encode(), tokFDs)
	tok.Close()
	if err != nil {
		return nil, false, err
	}
	link.SetDeadline(time.Time{})
	c, err = newConn(link, seg, int(lanes), int(ringSize), l.cfg.Spin, string(l.addr), true)
	if err != nil {
		return nil, true, err
	}
	return c, false, nil
}

// Close stops listening and removes the socket file; a blocked Accept
// returns net.ErrClosed. Connections already accepted stay open.
func (l *Listener) Close() error {
	l.closeOnce.Do(func() { l.closeErr = l.cl.Close() })
	return l.closeErr
}

// Addr returns the listener's socket path.
func (l *Listener) Addr() net.Addr { return l.addr }
