// Package ipc is a pure-Go shared-memory connection between two processes on
// one machine, for Linux, macOS and Windows.
//
// A Listener on an AF_UNIX socket path accepts the connections that Dial
// makes, any number of them, like a TCP server. A Conn is one pair of rings,
// one per direction, in a single shared-memory segment, so records move
// without system calls or copies; the socket carries only the handshake,
// doorbells that wake a parked peer, and goodbye. Conn is a net.Conn: Read
// and Write copy a byte stream over records, ReadFrom and WriteTo move it
// between the ring and another endpoint without a copy, and Reserve, Commit,
// Next and Release work on the records in place.
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
// and skipping empty ones. ReadFrom lets its source fill each record in
// place and WriteTo hands each record's slice to its destination. io.Copy
// calls a Conn's WriteTo when copying out of it, and its ReadFrom when
// copying into it unless the source has a WriteTo of its own, which io.Copy
// prefers. On one direction use either the record methods or the stream
// methods, Write and ReadFrom on the send side and Read and WriteTo on the
// receive side; interleaving them while Read or WriteTo holds a partly
// consumed record is not supported. The stream methods may be called from
// any goroutine; the record methods of one direction serve one goroutine at
// a time, one sending while another receives.
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
//
// # Costs
//
// A connection's segment holds its two rings, each RingSize bytes behind a
// 4 KiB header, 2 × (RingSize + 4096) bytes in all, and both processes map
// it. A Listener without ListenConfig.RingSize grants any dialer up to
// 512 MiB per direction. At most 16 connections are in their handshake or
// waiting for Accept at once, and those waiting hold their segments. On
// Linux, macOS and Windows a Listener holds the lock file path+".lock"
// beside its socket file; its Close removes it, except on Windows, where it
// stays.
//
// # Trust boundary
//
// Each end guards itself against its peer. It checks the ring headers and
// every record the peer publishes, and a violation ends the connection with
// ErrCorrupt. The peer cannot shrink the segment under this end's mapping.
// Descriptors it attaches to control frames are closed, save the segment's
// in the handshake. A doorbell the peer's control socket does not take
// within 50 ms ends the connection with a control protocol violation rather
// than holding up the call that rang it. A dialer that stalls or breaks its
// handshake never reaches Accept and keeps one of the 16 places for at most
// 5 s. Not guarded are the slices from Next, and those WriteTo hands its
// destination: they alias memory the peer can rewrite at any time, so copy
// what must not change before checking it. Nor does either end check who
// its peer is; the file system decides who may dial, and Listen makes the
// socket file mode 0600 on Linux and macOS.
package ipc
