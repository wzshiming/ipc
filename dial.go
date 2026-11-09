package ipc

import (
	"context"
	"fmt"
	"time"

	"github.com/wzshiming/ipc/internal/control"
	"github.com/wzshiming/ipc/internal/shm"
)

// Dial connects to the listener at path, requesting cfg; the connection
// uses the lanes and ring size the listener grants.
func Dial(ctx context.Context, path string, cfg Config) (*Conn, error) {
	cfg = cfg.withDefaults()
	link, err := control.Dial(ctx, path)
	if err != nil {
		return nil, err
	}
	c, err := dialHandshake(ctx, link, path, cfg)
	if err != nil {
		link.Close()
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		return nil, err
	}
	return c, nil
}

func dialHandshake(ctx context.Context, link *control.Link, path string, cfg Config) (*Conn, error) {
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
