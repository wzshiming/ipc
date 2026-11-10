// Package ipc is a pure-Go shared-memory connection between two processes on
// one machine, for Linux, macOS and Windows.
//
// A Listener on an AF_UNIX socket path accepts connections that Dial makes.
// Each Conn has Lanes() independent lanes; each lane has one ring per
// direction inside a single shared-memory segment, so records move without
// system calls or copies. The socket carries only the handshake, doorbells
// that wake a parked peer, and goodbye.
//
//	l, _ := (&ipc.ListenConfig{Lanes: 4}).Listen(ctx, "/tmp/app.sock")
//	c, _ := l.Accept()
//
//	c, _ := (&ipc.Dialer{Lanes: 4}).Dial("/tmp/app.sock")
//	lane := c.Lane(0)
//	p, _ := lane.Reserve(64) // write the record into p in place
//	lane.Commit()
//	rec, _ := lane.Next() // read the peer's record in place
//	lane.Release()
//
// A Dialer's Lanes and RingSize are a request: the listener clamps them to
// its ListenConfig and both ends use the result.
//
// Slices from Reserve and Next alias shared memory: a Reserve slice is valid
// until Commit or Abort, a Next slice until Release. Write and Read copy
// instead, one record per call. Each direction of a lane serves one goroutine
// at a time; use more lanes for more concurrency.
//
// Blocking calls busy-spin for the Spin duration without yielding their
// thread, then park until the peer rings, the lane's deadline passes or the
// connection closes; past the deadline they return os.ErrDeadlineExceeded.
// Close wakes them with net.ErrClosed. After the peer closes or dies, sends
// fail with ErrPeerClosed at once, while receives return the records it
// committed and then io.EOF after a graceful close or ErrPeerClosed after a
// crash, until this end's Close. The segment is unmapped once this end has
// closed and every outstanding slice has been released.
package ipc
