# ipc

`github.com/wzshiming/ipc` is a pure-Go (no cgo) shared-memory IPC library for two processes on one machine, on Linux, macOS and Windows. A connection is one pair of single-producer/single-consumer rings, one per direction, in one shared segment where records are written and read in place, so the data path makes no system call and no copy beyond the producer filling its record. `Conn` is a `net.Conn` that also exposes the records; throughput scales by opening more connections; an AF_UNIX socket carries only the handshake, the segment handle, doorbells and goodbye.

## Install

```shell
go get github.com/wzshiming/ipc
```

## API

Reference: https://pkg.go.dev/github.com/wzshiming/ipc

`Conn` is a `net.Conn`: Read and Write carry a byte stream through records, copying every byte, whereas `io.Copy` into or out of a `Conn` copies nothing, since `ReadFrom` has the source's Read fill each record in place and commits exactly the bytes it returned, and `WriteTo` hands each record's slice to the destination's Write. Beside the stream, the zero-copy record API — TryReserve, Reserve, Commit, Abort, TryNext, Next, Release — hands out slices that alias shared memory, a Reserve slice until Commit or Abort and a Next slice until Release; on one direction use either the record methods or the stream methods (Write and ReadFrom send, Read and WriteTo receive), not interleaved. `Listener` is a `net.Listener` whose Accept returns a `*Conn`; its SetDeadline bounds Accept and its Close removes the socket file but leaves accepted connections open. `Dialer.RingSize` is the ring capacity per direction the dialer requests, a power of two in [64 KiB, 512 MiB] (0 = 4 MiB); `ListenConfig.RingSize` caps what the listener grants (0 = no cap); both ends use the granted size, and `Conn.MaxRecord` is RingSize/2 − 8, a longer record failing with `ErrTooLarge`. `Spin` on either is how long a blocking call busy re-checks its ring before parking (0 = 50 µs, negative = park at once); `Dialer.Timeout` bounds the dial and handshake (0 = only the handshake's own 5 s bound), a refused handshake failing with `ErrRejected`; the context of `ListenConfig.Listen` and `Dialer.DialContext` bounds only the setup, and a nil receiver means the defaults. After the peer's Close or death, sends fail with `ErrPeerClosed` and receives drain what it committed, then return `io.EOF` after a Close or `ErrPeerClosed` after a death; after the local Close, blocked and later calls fail with `net.ErrClosed`. Deadlines are `net.Conn` deadlines: Write and Reserve honour the write deadline, Read and Next the read deadline, failing with `os.ErrDeadlineExceeded` (a zero time means none); TryReserve and TryNext never block and return `ErrWouldBlock` instead.

## Example

```go
l, _ := ipc.Listen(path) // path: an AF_UNIX socket path, ≤ 104 bytes on macOS
defer l.Close()
go func() {
	c, _ := l.Accept() // a net.Conn
	b := make([]byte, 5)
	io.ReadFull(c, b) // "hello"
	c.Write([]byte("hello, stream"))
	c.Close()
}()
c, _ := ipc.Dial(path) // a net.Conn
defer c.Close()
c.Write([]byte("hello"))
reply := make([]byte, 13)
io.ReadFull(c, reply)           // "hello, stream"
_, err := c.Read(reply)         // io.EOF once the listener's Close is drained
fmt.Println(string(reply), err) // hello, stream EOF
```

## Design

- One segment per connection, created by the listener during the handshake, holds two SPSC rings back to back: ring 0 carries listener → dialer, ring 1 dialer → listener. It is handed over the AF_UNIX socket as a Linux `memfd_create` fd or a macOS `shm_open` fd (random name, `shm_unlink`ed at once, libc called through a cgo-free trampoline) with `SCM_RIGHTS`, or on Windows by the name of a `CreateFileMapping` section in `Local\`.
- A ring is a 4096-byte header — magic `RING`, version, capacity, head, tail and two parked flags, each shared field on its own 128-byte line — plus a power-of-two data area in [64 KiB, 512 MiB]; a record is an 8-byte header (u32 length, u32 flags) plus its payload padded to 8 bytes, and one that does not fit before the end leaves a wrap marker and restarts at offset 0, hence `MaxRecord = RingSize/2 − 8`.
- Control protocol v2 frames on the socket: `Hello{version, ringSize}` from the dialer, `Welcome{ringSize, token}` (the request clamped to the listener's cap) or `Reject{reason}` from the listener, then only `Doorbell{dir}` and `Bye` either way; the handshake must finish within 5 s.
- A blocking Reserve or Next spins for Spin, then swaps its parked flag, re-checks the ring and blocks on a doorbell, the deadline or close; the committing or releasing side swaps the flag back and sends a doorbell only if the peer was parked, so no wakeup is lost even under plain acquire/release atomics such as the race detector's.
- Peer liveness rides on the socket: the peer's Close arrives as `Bye`, its crash as a socket error; `Conn.Done` closes and `Conn.Err` names the cause (`net.ErrClosed`, `ErrPeerClosed`, `ErrCorrupt` or the link error), and the segment is unmapped once neither half holds a slice.
- Each direction is one producer and one consumer, so one connection moves what two cores can and throughput scales with connections (`ipcbench -conns N`); the hot path allocates nothing and makes no system call except a doorbell to a parked peer.

## Performance

Measured by the CI bench job (`.github/workflows/ci.yml`) in master run [42808714536](https://github.com/wzshiming/ipc/actions/runs/42808714536): throughput at one connection with a 64 KiB record size through `io.Copy`, and the p50 of a 64 B round trip, per transport; GB = 10⁹ bytes. The summary of each bench job (one per runner) carries the full comparison (connections 1/2/4, record sizes 4 KiB to 1 MiB, `io.Copy` vs Write/Read, p99/p999) and is re-measured on every push to master on GitHub-hosted runners, whose hardware varies between runs; `go run ./cmd/ipcbench -mode compare -duration 1s` prints the same tables locally. `io.Copy` delivers ipc's records in place but copies AF_UNIX and TCP through its own 32 KiB buffer, and ipc's blocking calls spin for Spin (50 µs by default) before parking, whereas the kernel transports block in Go's netpoller.

| Runner | CPU | ipc GB/s | AF_UNIX GB/s | TCP GB/s | ipc p50 | AF_UNIX p50 | TCP p50 |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| ubuntu-latest | AMD EPYC 7763 64-Core Processor, 4 CPUs | 21.58 | 4.74 | 1.91 | 0.35 µs | 52.18 µs | 76.33 µs |
| macos-latest | Apple M1 (Virtual), 3 CPUs | 15.29 | 0.13 | 2.11 | 0.29 µs | 44.92 µs | 93.54 µs |
| windows-latest | AMD EPYC 7763 64-Core Processor, 4 CPUs | 21.07 | 2.46 | 2.14 | 0.40 µs | 71.40 µs | 77.60 µs |

## Platform notes and limitations

- Linux, macOS and Windows are supported; Windows needs Windows 10 1803 or later for AF_UNIX sockets. Other GOOS compile, but `Listener.Accept` fails with an error for which `errors.Is(err, errors.ErrUnsupported)` is true, and `Dial` then returns `ErrRejected`.
- On Linux and macOS the socket file is chmod 0600; macOS limits socket paths to 104 bytes and shared-memory names to 31 characters, which the library's random 29-character names fit.
- Read and Write may be called from any goroutine; the record methods of one direction serve one goroutine at a time, one sending while another receives.
- Windows' clock ticks in 0.5–15.6 ms steps, so `ipcbench` times latency with `QueryPerformanceCounter` and a Spin may last up to one timer tick before parking.
- Spin busy-waits a CPU for up to Spin per blocking call, 50 µs by default.
- Connections are not persistent: once a closed peer's records are drained nothing is replayed, and a socket file left behind by a dead listener is replaced by the next Listen.
