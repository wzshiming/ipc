package ipc

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/wzshiming/ipc/internal/control"
	"github.com/wzshiming/ipc/internal/shm"
)

// Dialer contains options for connecting to a Listener. Lanes and RingSize
// are a request: the listener clamps them to its ListenConfig and both ends
// use the clamped values.
type Dialer struct {
	// Lanes is the number of lanes requested; 0 means 1.
	Lanes int
	// RingSize is the ring capacity per lane direction requested in bytes, a
	// power of two in [64 KiB, 1 GiB]; 0 means 4 MiB.
	RingSize int
	// Spin is how long a blocking call on the connection busy re-checks its
	// lane, without yielding its thread, before parking; 0 means 50 µs and
	// a negative value parks at once. Each spinning call holds a CPU, so
	// choose Spin small or negative when CPUs are scarce.
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
	var (
		cfg     config
		timeout time.Duration
	)
	if d != nil {
		cfg = config{Lanes: d.Lanes, RingSize: d.RingSize, Spin: d.Spin}
		timeout = d.Timeout
	}
	cfg = cfg.withDefaults()
	dialCtx := ctx
	if timeout > 0 {
		var cancel context.CancelFunc
		dialCtx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	c, err := dial(dialCtx, path, cfg)
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

func dial(ctx context.Context, path string, cfg config) (*Conn, error) {
	link, err := control.Dial(ctx, path)
	if err != nil {
		return nil, err
	}
	c, err := dialHandshake(ctx, link, path, cfg)
	if err != nil {
		link.Close()
		return nil, err
	}
	return c, nil
}

func dialHandshake(ctx context.Context, link *control.Link, path string, cfg config) (*Conn, error) {
	stop := context.AfterFunc(ctx, func() { link.SetDeadline(time.Unix(1, 0)) })
	defer stop()
	link.SetDeadline(time.Now().Add(handshakeTimeout))
	h := hello{version: protoVersion, lanes: uint64(cfg.Lanes), ringSize: uint64(cfg.RingSize)}
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
	if uint64(tok.Size) != segmentSize(w.lanes, w.ringSize) {
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
	c, err := newConn(link, seg, int(w.lanes), int(w.ringSize), cfg.Spin, path, false)
	if err != nil {
		seg.Close()
		return nil, err
	}
	return c, nil
}
