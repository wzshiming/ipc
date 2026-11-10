// Package ipc is a pure-Go shared-memory connection between two processes on
// one machine, for Linux, macOS and Windows.
//
// A Listener on an AF_UNIX socket path accepts the connections that Dial
// makes, any number of them, like a TCP server. A Conn is one pair of rings,
// one per direction, in a single shared-memory segment, so records move
// without system calls or copies; the socket carries only the handshake,
// doorbells that wake a parked peer, and goodbye. Conn is a net.Conn: Read
// and Write copy a byte stream over records, io.Copy moves it through
// ReadFrom and WriteTo without a copy, and Reserve, Commit, Next and Release
// work on the records in place.
//
//	l, _ := ipc.Listen("/tmp/app.sock")
//	c, _ := l.Accept() // a net.Conn; c.(*ipc.Conn) adds the record API
//
//	c, _ := ipc.Dial("/tmp/app.sock")
//	p, _ := c.Reserve(64) // write the record into p in place
//	c.Commit()
//	rec, _ := c.Next() // read the peer's record in place
//	c.Release()
//
// The dialer chooses the ring capacity per direction (Dialer.RingSize,
// default 4 MiB) and the listener grants it, clamped to ListenConfig.RingSize
// when that is set. More bandwidth comes from more connections.
//
// Slices from Reserve and Next alias shared memory: a Reserve slice is valid
// until Commit or Abort, a Next slice until Release. Read and Write copy
// instead: Write splits its bytes into records of at most MaxRecord and Read
// returns their bytes in order, continuing a long record over several calls
// and skipping empty ones. io.Copy into or out of a Conn uses ReadFrom and
// WriteTo, which move the bytes between the ring and the other endpoint
// without a copy: ReadFrom lets the source fill each record in place and
// WriteTo hands each record's slice to the destination. On one direction use
// either the record methods or the stream methods, Write and ReadFrom on the
// send side and Read and WriteTo on the receive side; interleaving them while
// Read or WriteTo holds a partly consumed record is not supported. The stream
// methods may be called from any goroutine; the record methods of one
// direction serve one goroutine at a time, one sending while another
// receives.
//
// Blocking calls busy-spin for the Spin duration without yielding their
// thread, then park until the peer rings, the deadline passes or the
// connection closes; past the deadline they return os.ErrDeadlineExceeded, a
// net.Error whose Timeout method reports true. Close wakes them with
// net.ErrClosed. After the peer closes or dies, sends fail with ErrPeerClosed
// at once, while receives return the records it committed and then io.EOF
// after a graceful close or ErrPeerClosed after a crash, until this end's
// Close. The segment is unmapped once this end has closed and every
// outstanding slice has been released.
package ipc
