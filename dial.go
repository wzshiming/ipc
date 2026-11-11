package ipc

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/wzshiming/ipc/internal/control"
	"github.com/wzshiming/ipc/internal/shm"
)

// Dialer contains options for connecting to a Listener.
type Dialer struct {
	// RingSize is the ring capacity per direction requested in bytes, a
	// power of two in [64 KiB, 512 MiB]; 0 means 4 MiB. The listener grants
	// it, clamped to its ListenConfig.RingSize, and both ends use the result.
	RingSize int
	// Spin is how long a blocking call on the connection busy re-checks its
	// ring, without yielding its thread, before parking; 0 means 50 µs, or
	// none when runtime.NumCPU reports 1, and a negative value parks at
	// once. Each spinning call holds a CPU, so choose Spin small or negative
	// when CPUs are scarce.
	Spin time.Duration
	// Timeout is the maximum time the dial and handshake may take; 0 means
	// no limit other than the handshake's own 5 s bound.
	Timeout time.Duration
}

// Dial connects to the listener at path with the default Dialer.
func Dial(path string) (*Conn, error) {
	var d *Dialer
	return d.DialContext(context.Background(), path)
}

// Dial connects to the listener at path, as DialContext does with a
// background context.
func (d *Dialer) Dial(path string) (*Conn, error) {
	return d.DialContext(context.Background(), path)
}

// DialContext connects to the listener at path and completes the handshake.
// The dial fails when ctx is done or Timeout elapses, whichever comes first,
// returning ctx's error or os.ErrDeadlineExceeded; ctx does not affect the
// returned connection. It returns ErrRejected when the listener refuses the
// request. A nil d uses the defaults.
func (d *Dialer) DialContext(ctx context.Context, path string) (*Conn, error) {
	var cfg Dialer
	if d != nil {
		cfg = *d
	}
	if cfg.RingSize == 0 {
		cfg.RingSize = defaultRingSize
	}
	dialCtx := ctx
	if cfg.Timeout > 0 {
		var cancel context.CancelFunc
		dialCtx, cancel = context.WithTimeout(ctx, cfg.Timeout)
		defer cancel()
	}
	c, err := dial(dialCtx, path, cfg.RingSize, spinDuration(cfg.Spin))
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		if dialCtx.Err() != nil {
			return nil, os.ErrDeadlineExceeded
		}
		return nil, err
	}
	return c, nil
}

func dial(ctx context.Context, path string, ringSize int, spin time.Duration) (*Conn, error) {
	link, err := control.Dial(ctx, path)
	if err != nil {
		return nil, err
	}
	c, err := dialHandshake(ctx, link, path, ringSize, spin)
	if err != nil {
		link.Close()
		return nil, err
	}
	return c, nil
}

func dialHandshake(ctx context.Context, link *control.Link, path string, ringSize int, spin time.Duration) (*Conn, error) {
	stop := context.AfterFunc(ctx, func() { link.SetDeadline(time.Unix(1, 0)) })
	defer stop()
	link.SetDeadline(time.Now().Add(handshakeTimeout))
	h := hello{version: protoVersion, ringSize: uint64(ringSize)}
	if err := link.Send(frameHello, h.encode(), nil); err != nil {
		return nil, err
	}
	typ, p, fds, err := link.Recv()
	if err != nil {
		return nil, err
	}
	switch typ {
	case frameWelcome:
	case frameReject:
		discardFDs(fds)
		return nil, fmt.Errorf("%w: %q", ErrRejected, p)
	default:
		discardFDs(fds)
		return nil, errProtocol
	}
	w, err := decodeWelcome(p)
	if err != nil {
		discardFDs(fds)
		return nil, err
	}
	tok, err := shm.DecodeToken(w.token, fds)
	if err != nil {
		return nil, err
	}
	if uint64(tok.Size) != segmentSize(w.ringSize) {
		tok.Close()
		return nil, errProtocol
	}
	seg, err := shm.Attach(tok)
	if err != nil {
		return nil, err
	}
	if !stop() {
		seg.Close()
		return nil, ctx.Err()
	}
	link.SetDeadline(time.Time{})
	c, err := newConn(link, seg, int(w.ringSize), spin, path, false)
	if err != nil {
		seg.Close()
		return nil, err
	}
	return c, nil
}
