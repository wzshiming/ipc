// Package ipc is a pure-Go shared-memory connection between two processes on
// one machine, for Linux, macOS and Windows.
//
// A Listener on an AF_UNIX socket path accepts connections that Dial makes.
// Each Conn has Lanes() independent lanes; each lane has one ring per
// direction inside a single shared-memory segment, so records move without
// system calls or copies. The socket carries only the handshake, doorbells
// that wake a parked peer, and goodbye.
//
//	l, _ := ipc.Listen("/tmp/app.sock", ipc.Config{Lanes: 4})
//	c, _ := l.Accept(ctx)
//
//	c, _ := ipc.Dial(ctx, "/tmp/app.sock", ipc.Config{Lanes: 4})
//	lane := c.Lane(0)
//	p, _ := lane.Reserve(ctx, 64) // write the record into p in place
//	lane.Commit()
//	rec, _ := lane.Next(ctx) // read the peer's record in place
//	lane.Release()
//
// The dialer's Config is a request: the listener clamps lanes and ring size
// to its own Config and both ends use the result.
//
// Slices from Reserve and Next alias shared memory: a Reserve slice is valid
// until Commit or Abort, a Next slice until Release. Send and Recv copy
// instead. Each direction of a lane serves one goroutine at a time; use more
// lanes for more concurrency.
//
// Blocking calls spin for Config.Spin, then park until the peer rings,
// ctx is done or the connection closes. Close wakes them with ErrClosed on
// this end and ErrPeerClosed on the other, as does the peer's death. The
// segment is unmapped once every outstanding slice has been released.
package ipc
