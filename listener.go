package ipc

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"sync"
	"time"

	"github.com/wzshiming/ipc/internal/control"
	"github.com/wzshiming/ipc/internal/ring"
	"github.com/wzshiming/ipc/internal/shm"
)

// backlog is the most connections in their handshake or waiting for Accept
// at once; further dialers wait in the kernel's listen backlog.
const backlog = 16

// createSegment is shm.Create, replaced in tests.
var createSegment = shm.Create

// ListenConfig contains options for listening.
type ListenConfig struct {
	// RingSize caps the ring capacity per direction a connection gets: a
	// dialer's request above it is clamped to it. It is a power of two in
	// [64 KiB, 512 MiB]; 0 means no cap.
	RingSize int
	// Spin is how long a blocking call on an accepted connection busy
	// re-checks its ring, without yielding its thread, before parking; 0
	// means 50 µs, or none when runtime.NumCPU reports 1, and a negative
	// value parks at once. Each spinning call holds a CPU, so choose Spin
	// small or negative when CPUs are scarce.
	Spin time.Duration
}

// Listener accepts connections on an AF_UNIX socket path, any number of
// them. It is a net.Listener whose Accept returns a *Conn. From Listen on it
// takes dialers and runs their handshakes in the background, side by side,
// so a Dial can complete before Accept is called; the connections then wait
// for Accept in the order their handshakes completed. At most 16 connections
// are in their handshake or waiting for Accept at once; further dialers wait
// in the socket's backlog until Accept takes one.
type Listener struct {
	cl       *control.Listener
	ringSize int // the largest ring size granted
	spin     time.Duration
	addr     addr

	ctx    context.Context // done once Close begins
	cancel context.CancelFunc
	slots  chan struct{}  // one per connection in its handshake or waiting for Accept
	ready  chan outcome   // completed handshakes, in completion order
	dl     deadline       // Accept's deadline
	wg     sync.WaitGroup // serve and the handshakes it started

	closeOnce sync.Once
	closeErr  error
}

// outcome is a completed handshake: a connection, or a failure of this end.
type outcome struct {
	c   *Conn
	err error
}

var _ net.Listener = (*Listener)(nil)

// Listen listens on the AF_UNIX socket at path with the default
// ListenConfig. A socket file left behind by a dead listener is replaced.
func Listen(path string) (*Listener, error) {
	var lc *ListenConfig
	return lc.Listen(context.Background(), path)
}

// Listen listens on the AF_UNIX socket at path. It fails if lc's RingSize is
// out of range or another listener is live on path; a socket file left
// behind by a dead listener is replaced. ctx bounds the setup, including the
// probe of an existing socket file, but not the returned Listener. A nil lc
// uses the defaults.
func (lc *ListenConfig) Listen(ctx context.Context, path string) (*Listener, error) {
	var cfg ListenConfig
	if lc != nil {
		cfg = *lc
	}
	if cfg.RingSize == 0 {
		cfg.RingSize = maxRingSize
	}
	if !validRingSize(uint64(cfg.RingSize)) {
		return nil, fmt.Errorf("ipc: RingSize %d is not a power of two in [64 KiB, 512 MiB]", cfg.RingSize)
	}
	cl, err := control.Listen(ctx, path)
	if err != nil {
		return nil, err
	}
	l := &Listener{
		cl:       cl,
		ringSize: cfg.RingSize,
		spin:     spinDuration(cfg.Spin),
		addr:     addr(path),
		slots:    make(chan struct{}, backlog),
		ready:    make(chan outcome, backlog),
	}
	l.ctx, l.cancel = context.WithCancel(context.Background())
	l.wg.Add(1)
	go l.serve()
	return l, nil
}

// serve takes dialers from the socket and starts their handshakes until
// Close, holding a slot per dialer until Accept takes its outcome or its
// handshake is dropped. A failure to take a dialer is this end's and reaches
// Accept like one of a handshake.
func (l *Listener) serve() {
	defer l.wg.Done()
	for {
		select {
		case l.slots <- struct{}{}:
		case <-l.ctx.Done():
			return
		}
		link, err := l.cl.Accept()
		if err != nil {
			if errors.Is(err, control.ErrClosed) {
				return
			}
			l.ready <- outcome{err: err}
			continue
		}
		l.wg.Add(1)
		go l.admit(link)
	}
}

// admit runs link's handshake within the handshake limit and queues its
// outcome for Accept, dropping a failure the dialer caused together with its
// slot. Close aborts the handshake with a past deadline, which one that
// completed meanwhile survives, its connection then closed by Close.
func (l *Listener) admit(link *control.Link) {
	defer l.wg.Done()
	link.SetDeadline(time.Now().Add(handshakeTimeout))
	aborted := make(chan struct{})
	stop := context.AfterFunc(l.ctx, func() {
		link.SetDeadline(time.Unix(1, 0))
		close(aborted)
	})
	c, local, err := l.handshake(link)
	if !stop() {
		<-aborted
	}
	switch {
	case err == nil:
		link.SetDeadline(time.Time{})
		c.start()
		l.ready <- outcome{c: c}
	case local:
		l.ready <- outcome{err: err}
	default:
		<-l.slots
	}
}

// Accept waits for the next connection whose handshake completed and returns
// it, a *Conn; connections wait in the order their handshakes completed.
// Dialers that fail the handshake never reach Accept, while a failure of
// this end, such as one to create a segment, is returned in place of a
// connection and the Listener serves on. Past the deadline set by
// SetDeadline it returns os.ErrDeadlineExceeded, and after Close
// net.ErrClosed.
func (l *Listener) Accept() (net.Conn, error) {
	if l.ctx.Err() != nil {
		return nil, net.ErrClosed
	}
	if l.dl.expired.Load() {
		return nil, os.ErrDeadlineExceeded
	}
	select {
	case o, ok := <-l.ready:
		if !ok {
			return nil, net.ErrClosed
		}
		<-l.slots
		if l.ctx.Err() != nil { // Close began: it hands out no connection
			if o.c != nil {
				o.c.Close()
			}
			return nil, net.ErrClosed
		}
		if o.err != nil {
			return nil, o.err
		}
		return o.c, nil
	case <-l.ctx.Done():
		return nil, net.ErrClosed
	case <-l.dl.done():
		return nil, os.ErrDeadlineExceeded
	}
}

// SetDeadline sets the deadline for Accept, including a call already
// blocked; a zero t means no deadline. It does not bound the handshakes,
// which run in the background, each within its own 5 s limit.
func (l *Listener) SetDeadline(t time.Time) error {
	if l.ctx.Err() != nil {
		return net.ErrClosed
	}
	l.dl.set(t)
	return nil
}

// handshake answers the dialer's Hello and returns the connection, its
// control reader not yet started. local reports a failure of this end rather
// than the dialer's. The link is closed on failure.
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
	typ, p, fds, err := link.Recv()
	if err != nil {
		return nil, false, err
	}
	control.CloseFDs(fds)
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
	ringSize := min(h.ringSize, uint64(l.ringSize))
	seg, err = createSegment(int(segmentSize(ringSize)))
	if err != nil {
		link.Send(frameReject, []byte("cannot create segment"), nil)
		return nil, true, err
	}
	mem, rs := seg.Bytes(), ring.Size(int(ringSize))
	for k := range 2 {
		if err := ring.Format(mem[k*rs:(k+1)*rs], int(ringSize)); err != nil {
			return nil, true, err
		}
	}
	// Wired before the dialer holds the segment, so nothing it writes there fails this end.
	c, err = newConn(link, seg, int(ringSize), l.spin, string(l.addr), true)
	if err != nil {
		return nil, true, err
	}
	tok, err := seg.Export()
	if err != nil {
		return nil, true, err
	}
	payload, tokFDs := tok.Encode()
	err = link.Send(frameWelcome, welcome{ringSize: ringSize, token: payload}.encode(), tokFDs)
	tok.Close()
	if err != nil {
		return nil, false, err
	}
	return c, false, nil
}

// Close stops taking dialers, aborts the handshakes in flight, closes the
// connections waiting for Accept, whose dialers see the connection end, and
// removes the socket file; a blocked Accept returns net.ErrClosed. It returns
// once the listener's goroutines have ended. Connections Accept returned
// stay open.
func (l *Listener) Close() error {
	l.closeOnce.Do(func() {
		l.cancel()
		l.closeErr = l.cl.Close()
		l.wg.Wait()
		close(l.ready)
		// Side by side, as each may wait up to readerWait for its dialer.
		var closing sync.WaitGroup
		for o := range l.ready {
			if o.c != nil {
				closing.Go(func() { o.c.Close() })
			}
		}
		closing.Wait()
	})
	return l.closeErr
}

// Addr returns the listener's socket path.
func (l *Listener) Addr() net.Addr { return l.addr }
