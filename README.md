# ipc

`github.com/wzshiming/ipc` is a pure-Go (no cgo) shared-memory IPC library for two processes on one machine, on Linux, macOS and Windows. A connection is one pair of single-producer/single-consumer rings, one per direction, in one shared segment where records are written and read in place, so the data path makes no system call and no copy beyond the producer filling its record. `Conn` is a `net.Conn` that also exposes the records; throughput scales by opening more connections; an AF_UNIX socket carries only the handshake, the segment handle, doorbells and goodbye.

## Install

```shell
go get github.com/wzshiming/ipc
```

## API

```go
func Listen(path string) (*Listener, error)
type ListenConfig struct {
	RingSize int
	Spin time.Duration
}
func (lc *ListenConfig) Listen(ctx context.Context, path string) (*Listener, error)
func (l *Listener) Accept() (net.Conn, error)
func (l *Listener) SetDeadline(t time.Time) error
func (l *Listener) Close() error
func (l *Listener) Addr() net.Addr
func Dial(path string) (*Conn, error)
type Dialer struct {
	RingSize int
	Spin time.Duration
	Timeout time.Duration
}
func (d *Dialer) Dial(path string) (*Conn, error)
func (d *Dialer) DialContext(ctx context.Context, path string) (*Conn, error)
func (c *Conn) Read(p []byte) (int, error)
func (c *Conn) Write(p []byte) (int, error)
func (c *Conn) Close() error
func (c *Conn) LocalAddr() net.Addr
func (c *Conn) RemoteAddr() net.Addr
func (c *Conn) SetDeadline(t time.Time) error
func (c *Conn) SetReadDeadline(t time.Time) error
func (c *Conn) SetWriteDeadline(t time.Time) error
func (c *Conn) TryReserve(n int) ([]byte, error)
func (c *Conn) Reserve(n int) ([]byte, error)
func (c *Conn) Commit() error
func (c *Conn) Abort()
func (c *Conn) TryNext() ([]byte, error)
func (c *Conn) Next() ([]byte, error)
func (c *Conn) Release() error
func (c *Conn) RingSize() int
func (c *Conn) MaxRecord() int
func (c *Conn) Done() <-chan struct{}
func (c *Conn) Err() error
var (
	ErrPeerClosed = errors.New("ipc: peer closed")
	ErrWouldBlock = errors.New("ipc: operation would block")
	ErrTooLarge = errors.New("ipc: record larger than MaxRecord")
	ErrCorrupt = errors.New("ipc: shared memory corrupt")
	ErrRejected = errors.New("ipc: rejected by listener")
)
```

`Listener` is a `net.Listener` whose Accept returns a `*Conn`, a `net.Conn`. Reserve/Commit (or Abort) and Next/Release are zero-copy: their slices alias shared memory, a Reserve slice until Commit or Abort and a Next slice until Release. Write splits its bytes into records of at most MaxRecord and Read returns their bytes in order, continuing a long record over several calls and skipping empty ones. TryReserve and TryNext never block and return `ErrWouldBlock`; Reserve/Write honour the write deadline and Next/Read the read deadline, as on a `net.Conn` (`Listener.SetDeadline` covers Accept), and a zero time means none. The context of `ListenConfig.Listen` and `Dialer.DialContext` bounds only the setup, and a nil receiver uses the defaults.

## Example

```go
// The listener and the dialer are normally two processes.
func Example() {
	dir, _ := os.MkdirTemp("", "ipc") // short: sun_path is ≤ 104 bytes on macOS
	defer os.RemoveAll(dir)
	path := filepath.Join(dir, "s")
	l, err := ipc.Listen(path)
	if err != nil {
		log.Fatal(err)
	}
	defer l.Close()
	done := make(chan struct{})
	go func() {
		defer close(done)
		c, err := ipc.Dial(path)
		if err != nil {
			log.Fatal(err)
		}
		defer c.Close()
		p, err := c.Reserve(len("hello")) // the record API: p is shared memory
		if err != nil {
			log.Fatal(err)
		}
		copy(p, "hello")
		c.Commit()
		b, err := io.ReadAll(c) // the byte stream the listener sends, until its Close
		fmt.Printf("dialer read %q, %v\n", b, err)
	}()
	nc, err := l.Accept() // a net.Conn
	if err != nil {
		log.Fatal(err)
	}
	c := nc.(*ipc.Conn) // *Conn adds the record API
	rec, _ := c.Next()  // rec aliases the ring until Release
	fmt.Printf("listener got %q\n", rec)
	c.Release()
	io.WriteString(c, "hello, stream")
	c.Close() // the dialer's Read sees io.EOF
	<-done
	// Output:
	// listener got "hello"
	// dialer read "hello, stream", <nil>
}
```

## Design

- One segment per connection, created by the listener, holds two SPSC rings back to back: ring 0 carries listener → dialer, ring 1 dialer → listener. The ring size per direction is a power of two in [64 KiB, 512 MiB] (a segment holds two of them): the dialer requests it (Dialer.RingSize, 0 = 4 MiB), the listener clamps the request to ListenConfig.RingSize when that is set (0 = no cap), and both ends use the granted value.
- Each ring starts with a 4096-byte header (magic `RING`, version, capacity, head, tail and the two parked flags), each shared field on its own 128-byte line so producer and consumer never share a cache line.
- A record is an 8-byte header (u32 length, u32 flags) plus the payload padded to 8 bytes; one that does not fit before the end leaves a wrap marker and restarts at offset 0, hence `MaxRecord = RingSize/2 − 8`.
- Segment creation and hand-over: Linux `memfd_create`, fd passed with `SCM_RIGHTS`; macOS `shm_open` under a random name `shm_unlink`ed immediately (libc called through a cgo-free trampoline), fd passed with `SCM_RIGHTS`; Windows named `CreateFileMapping` in `Local\` with a random 128-bit name, passed by name.
- Control protocol v2 frames: `Hello{version, ringSize}` from the dialer, `Welcome{ringSize, token}` or `Reject{reason}` from the listener, then `Doorbell{dir}` and `Bye` either way; the handshake is bounded by 5 s (Dialer.Timeout may shorten it).
- A blocking Reserve or Next spins for Spin (default 50 µs, negative parks at once), then swaps its parked flag, re-checks the ring and blocks on a doorbell, the deadline or close.
- The committing (or releasing) side swaps the same flag back and sends a doorbell only if the peer was parked, so no wakeup is lost even under plain acquire/release atomics such as the race detector's.
- After the peer closes, sends fail at once with `ErrPeerClosed` and receives drain what it committed, then return `io.EOF` (graceful close) or `ErrPeerClosed` (crash); a local Close wakes blocked calls with `net.ErrClosed`; timeouts return `os.ErrDeadlineExceeded`. `Conn.Done` and `Conn.Err` report the cause.
- Each direction of a connection is one producer and one consumer, so one connection moves what two cores can; throughput scales by opening more connections, as `ipcbench -conns N` does.
- The hot path makes zero allocations and no system calls except doorbells to a parked peer.

## Performance

Measured on 2026-11-10 on an Apple M1 (4 performance + 4 efficiency cores), macOS 26.6, Go 1.27.3, two 5-second runs each with producer and consumer pinned by `runtime.LockOSThread`; GB = 10⁹ bytes released by the consumer, the producer filling every byte of each record (the single copy) and the consumer reading one word per 128 bytes. The host was busy: the 1-minute load average read right before each run was 4.2–4.3 for the first row and 6.4–14.9 (≥ 5) for every other row, so the numbers are indicative, not a quiet-machine ceiling. 1 Tbps (125 GB/s) is a design target, not a measurement: each connection needs two pinned cores, so 2 connections already fill this host's 4 performance cores and more connections only share them.

| Command | Run 1 | Run 2 | Note |
| --- | --- | --- | --- |
| `go run ./cmd/ipcbench -mode throughput -size 1MiB -conns 1` | 23.60 GB/s (188.8 Gbps) | 9.96 GB/s (79.7 Gbps) | |
| `go run ./cmd/ipcbench -mode throughput -size 1MiB -conns 2` | 28.63 GB/s (229 Gbps) | 29.97 GB/s (240 Gbps) | ≈ 14–15 GB/s per connection |
| `go run ./cmd/ipcbench -mode throughput -size 1MiB -conns 4` | 16.32 GB/s (131 Gbps) | 20.62 GB/s (165 Gbps) | ≈ 4–5.5 GB/s per connection; 8 pinned threads on 8 cores |
| `go run ./cmd/ipcbench -mode throughput -size 1MiB -conns 8` | 15.18 GB/s (122 Gbps) | 9.71 GB/s (78 Gbps) | ≈ 1.1–2 GB/s per connection; 16 pinned threads on 8 cores |
| `go run ./cmd/ipcbench -mode throughput -conns 8 -size 4MiB -ring 16MiB` | 8.88 GB/s | 8.39 GB/s | |
| `go run ./cmd/ipcbench -mode framing -size 64B` | 44.86 M records/s | 39.30 M records/s | tight spin; producer and consumer one record apart, so the head index crosses cores per record |
| `go run ./cmd/ipcbench -mode framing -size 64B -spin -1ns` | 48.36 M records/s | | parks at once, which batches |
| `go run ./cmd/ipcbench -mode latency` | p50 0.21 µs, p99 0.75 µs, p999 12.75 µs, min 0.08 µs | | 64 B ping-pong, 1 connection |

## Platform notes and limitations

- Windows needs Windows 10 1803 or later for AF_UNIX sockets.
- On Linux and macOS the socket file is chmod 0600; macOS limits socket paths to 104 bytes and shared-memory names to 31 characters, which the library's random names fit (internal detail).
- Other GOOS compile, but `Listener.Accept` fails with an error for which `errors.Is(err, errors.ErrUnsupported)` is true, and `Dial` then returns `ErrRejected`.
- Windows' clock ticks in 0.5–15.6 ms steps, so `ipcbench` times latency with `QueryPerformanceCounter` and a Spin may last up to one timer tick before parking.
- Read and Write may be called from any goroutine; the record methods of one direction serve one goroutine at a time, one sending while another receives.
- On one direction use either the record methods or Read/Write; interleaving them while Read holds a partly consumed record is not supported.
- Spin busy-waits a CPU for up to Spin per blocking call; set it small or negative when CPUs are scarce.
- Connections are not persistent: once a closed peer's records are drained, nothing is replayed.
