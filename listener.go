package ipc

import (
	"context"
	"errors"
	"net"
	"sync"
	"time"

	"github.com/wzshiming/ipc/internal/control"
	"github.com/wzshiming/ipc/internal/ring"
	"github.com/wzshiming/ipc/internal/shm"
)

// Listener accepts connections on a socket path.
type Listener struct {
	cl   *control.Listener
	cfg  Config
	addr addr

	links     chan accepted
	done      chan struct{}
	closeOnce sync.Once
	closeErr  error
	wg        sync.WaitGroup
}

type accepted struct {
	link *control.Link
	err  error
}

// Listen listens on the AF_UNIX socket at path. cfg sets the limits and
// defaults for accepted connections.
func Listen(path string, cfg Config) (*Listener, error) {
	cfg = cfg.withDefaults()
	if err := cfg.checkListener(); err != nil {
		return nil, err
	}
	cl, err := control.Listen(context.Background(), path)
	if err != nil {
		return nil, err
	}
	l := &Listener{
		cl:    cl,
		cfg:   cfg,
		addr:  addr(path),
		links: make(chan accepted),
		done:  make(chan struct{}),
	}
	l.wg.Add(1)
	go l.acceptLoop()
	return l, nil
}

// acceptLoop hands raw links to Accept, holding at most one at a time.
func (l *Listener) acceptLoop() {
	defer l.wg.Done()
	for {
		link, err := l.cl.Accept()
		if errors.Is(err, control.ErrClosed) {
			return
		}
		select {
		case l.links <- accepted{link, err}:
		case <-l.done:
			if link != nil {
				link.Close()
			}
			return
		}
	}
}

// Accept waits for a dialer and completes its handshake. Dialers that fail
// the handshake are dropped and Accept keeps waiting.
func (l *Listener) Accept(ctx context.Context) (*Conn, error) {
	for {
		select {
		case a := <-l.links:
			if a.err != nil {
				return nil, a.err
			}
			c, local, err := l.handshake(ctx, a.link)
			if err == nil {
				return c, nil
			}
			if local {
				return nil, err
			}
			if err := ctx.Err(); err != nil {
				return nil, err
			}
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-l.done:
			return nil, ErrClosed
		}
	}
}

// handshake answers the dialer's Hello. local reports a failure on this end
// rather than the dialer's. The link is closed on failure.
func (l *Listener) handshake(ctx context.Context, link *control.Link) (c *Conn, local bool, err error) {
	stop := context.AfterFunc(ctx, func() { link.SetDeadline(time.Unix(1, 0)) })
	var seg *shm.Segment
	defer func() {
		if err != nil {
			stop()
			link.Close()
			if seg != nil {
				seg.Close()
			}
		}
	}()
	link.SetDeadline(time.Now().Add(handshakeTimeout))
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
	if !stop() {
		return nil, false, ctx.Err()
	}
	link.SetDeadline(time.Time{})
	c, err = newConn(link, seg, int(lanes), int(ringSize), l.cfg.Spin, string(l.addr), true)
	if err != nil {
		return nil, true, err
	}
	return c, false, nil
}

// Close stops listening; connections already accepted stay open.
func (l *Listener) Close() error {
	l.closeOnce.Do(func() {
		close(l.done)
		l.closeErr = l.cl.Close()
		l.wg.Wait()
	})
	return l.closeErr
}

// Addr returns the listener's socket path.
func (l *Listener) Addr() net.Addr { return l.addr }
