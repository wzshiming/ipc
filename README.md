# ipc

`github.com/wzshiming/ipc` is a pure-Go (no cgo) shared-memory IPC library for two processes on one machine, on Linux, macOS and Windows. A connection is a set of independent lanes, each a pair of single-producer/single-consumer rings in one shared segment where records are written and read in place, so the data path makes no system call and no copy beyond the producer filling its record. Throughput scales by adding lanes; an AF_UNIX socket carries only the handshake, the segment handle, doorbells and goodbye.

## Install

```shell
go get github.com/wzshiming/ipc
```

## API

```go
func Listen(path string) (*Listener, error)
type ListenConfig struct {
	Lanes int
	RingSize int
	Spin time.Duration
}
func (lc *ListenConfig) Listen(ctx context.Context, path string) (*Listener, error)
func (l *Listener) Accept() (*Conn, error)
func (l *Listener) SetDeadline(t time.Time) error
func (l *Listener) Close() error
func (l *Listener) Addr() net.Addr
func Dial(path string) (*Conn, error)
type Dialer struct {
	Lanes int
	RingSize int
	Spin time.Duration
	Timeout time.Duration
}
func (d *Dialer) Dial(path string) (*Conn, error)
func (d *Dialer) DialContext(ctx context.Context, path string) (*Conn, error)
func (c *Conn) Lanes() int
func (c *Conn) Lane(i int) *Lane
func (c *Conn) RingSize() int
func (c *Conn) MaxRecord() int
func (c *Conn) LocalAddr() net.Addr
func (c *Conn) RemoteAddr() net.Addr
func (c *Conn) Close() error
func (c *Conn) Done() <-chan struct{}
func (c *Conn) Err() error
func (l *Lane) Conn() *Conn
func (l *Lane) Index() int
func (l *Lane) TryReserve(n int) ([]byte, error)
func (l *Lane) Reserve(n int) ([]byte, error)
func (l *Lane) Commit() error
func (l *Lane) Abort()
func (l *Lane) TryNext() ([]byte, error)
func (l *Lane) Next() ([]byte, error)
func (l *Lane) Release() error
func (l *Lane) Write(p []byte) (int, error)
func (l *Lane) Read(p []byte) (int, error)
func (l *Lane) SetDeadline(t time.Time) error
func (l *Lane) SetReadDeadline(t time.Time) error
func (l *Lane) SetWriteDeadline(t time.Time) error
func NewStream(l *Lane) net.Conn
var (
	ErrPeerClosed = errors.New("ipc: peer closed")
	ErrWouldBlock = errors.New("ipc: operation would block")
	ErrTooLarge = errors.New("ipc: record larger than MaxRecord")
	ErrCorrupt = errors.New("ipc: shared memory corrupt")
	ErrRejected = errors.New("ipc: rejected by listener")
)
```

Reserve/Commit (or Abort) and Next/Release are zero-copy: their slices alias shared memory, a Reserve slice until Commit or Abort and a Next slice until Release, while Write and Read copy one record per call (a longer record fills `p` and yields `io.ErrShortBuffer`). TryReserve and TryNext never block and return `ErrWouldBlock`; Reserve/Write honour the lane's write deadline and Next/Read its read deadline, as on a `net.Conn` (`Listener.SetDeadline` covers Accept), and a zero time means none. The context of `ListenConfig.Listen` and `Dialer.DialContext` bounds only the setup, and a nil receiver uses the defaults.

## Example

```go
// The listener and the dialer are normally two processes.
func Example() {
	dir, _ := os.MkdirTemp("", "ipc") // short: sun_path is ≤ 104 bytes on macOS
	defer os.RemoveAll(dir)
	path := filepath.Join(dir, "s")
	l, err := (&ipc.ListenConfig{Lanes: 2}).Listen(context.Background(), path)
	if err != nil {
		log.Fatal(err)
	}
	defer l.Close()
	go func() {
		c, err := (&ipc.Dialer{Lanes: 2}).Dial(path)
		if err != nil {
			log.Fatal(err)
		}
		defer c.Close()
		p, err := c.Lane(0).Reserve(len("hello"))
		if err != nil {
			log.Fatal(err)
		}
		copy(p, "hello") // fill the record in shared memory
		c.Lane(0).Commit()
		s := ipc.NewStream(c.Lane(1)) // lane 1 as a net.Conn byte stream
		io.WriteString(s, "hello, stream")
		s.Close() // the peer's Read sees io.EOF
	}()
	c, err := l.Accept()
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()
	rec, _ := c.Lane(0).Next() // rec aliases the ring until Release
	fmt.Printf("%d lanes, lane 0: %q\n", c.Lanes(), rec)
	c.Lane(0).Release()
	b, err := io.ReadAll(ipc.NewStream(c.Lane(1)))
	fmt.Printf("lane 1: %q, %v\n", b, err)
	// Output:
	// 2 lanes, lane 0: "hello"
	// lane 1: "hello, stream", <nil>
}
```

## Design

- One segment per connection, created by the listener, holds 2 × Lanes SPSC rings, each starting page-aligned: lane *i* uses ring 2*i* (listener → dialer) and 2*i*+1 (dialer → listener). RingSize is a power of two in [64 KiB, 1 GiB], default 4 MiB; Lanes defaults to 1.
- Each ring starts with a 4096-byte header (magic `RING`, version, capacity, head, tail and the two parked flags), each shared field on its own 128-byte line so producer and consumer never share a cache line.
- A record is an 8-byte header (u32 length, u32 flags) plus the payload padded to 8 bytes; one that does not fit before the end leaves a wrap marker and restarts at offset 0, hence `MaxRecord = RingSize/2 − 8`.
- Segment creation and hand-over: Linux `memfd_create`, fd passed with `SCM_RIGHTS`; macOS `shm_open` under a random name `shm_unlink`ed immediately (libc called through a cgo-free trampoline), fd passed with `SCM_RIGHTS`; Windows named `CreateFileMapping` in `Local\` with a random 128-bit name, passed by name.
- Control protocol v1 frames: `Hello{version, lanes, ringSize}` from the dialer, `Welcome{lanes, ringSize, token}` or `Reject{reason}` from the listener, then `Doorbell{lane, dir}` and `Bye` either way. The listener clamps the Dialer's Lanes and RingSize to its ListenConfig and both ends use the granted values; the handshake is bounded by 5 s (Dialer.Timeout may shorten it).
- A blocking Reserve or Next spins for Spin (default 50 µs, negative parks at once), then swaps its parked flag, re-checks the ring and blocks on a doorbell, the deadline or close.
- The committing (or releasing) side swaps the same flag back and sends a doorbell only if the peer was parked, so no wakeup is lost even under plain acquire/release atomics such as the race detector's.
- After the peer closes, sends fail at once with `ErrPeerClosed` and receives drain what it committed, then return `io.EOF` (graceful close) or `ErrPeerClosed` (crash); a local Close wakes blocked calls with `net.ErrClosed`; timeouts return `os.ErrDeadlineExceeded`. `Conn.Done` and `Conn.Err` report the cause.
- The hot path makes zero allocations and no system calls except doorbells to a parked peer.

## Performance

Measured on 2026-11-09 on an Apple M1 Max (8 performance + 2 efficiency cores), macOS, Go 1.27.3, two 5-second runs each with producer and consumer pinned by `runtime.LockOSThread`; GB = 10⁹ bytes released by the consumer, the producer filling every byte of each record (the single copy) and the consumer reading one word per 128 bytes. 1 Tbps (125 GB/s) is a design target, not a measurement: 4 lanes plateau near 0.6 Tbps here because each lane needs two pinned cores, so at ≈ 18 GB/s per lane it needs about 7 lanes on ≥ 14 performance cores with ≈ 250 GB/s of memory bandwidth.

| Command | Run 1 | Run 2 | Note |
| --- | --- | --- | --- |
| `go run ./cmd/ipcbench -mode throughput -size 1MiB -lanes 1` | 34.48 GB/s (275.8 Gbps) | 34.62 GB/s (277.0 Gbps) | |
| `go run ./cmd/ipcbench -mode throughput -size 1MiB -lanes 2` | 55.76 GB/s (446 Gbps) | 56.98 GB/s (456 Gbps) | ≈ 28 GB/s per lane |
| `go run ./cmd/ipcbench -mode throughput -size 1MiB -lanes 4` | 72.93 GB/s (584 Gbps) | 75.20 GB/s (602 Gbps) | ≈ 18–19 GB/s per lane |
| `go run ./cmd/ipcbench -mode throughput -size 1MiB -lanes 8` | 67.11 GB/s (537 Gbps) | 65.41 GB/s (523 Gbps) | ≈ 8–8.5 GB/s per lane; 16 pinned threads on 10 cores |
| `go run ./cmd/ipcbench -mode throughput -lanes 8 -size 4MiB -ring 16MiB` | 35.66 GB/s | 37.01 GB/s | |
| `go run ./cmd/ipcbench -mode framing -size 64B` | 22.84 M records/s | 22.68 M records/s | tight spin; producer and consumer one record apart, so the head index crosses cores per record |
| `go run ./cmd/ipcbench -mode framing -size 64B -spin -1ns` | 59.0 M records/s | | parks at once, which batches |
| `go run ./cmd/ipcbench -mode latency` | p50 0.21 µs, p99 0.83 µs, p999 0.96–1.00 µs, min 0.08 µs | | 64 B ping-pong, 1 lane |

## Platform notes and limitations

- Windows needs Windows 10 1803 or later for AF_UNIX sockets.
- On Linux and macOS the socket file is chmod 0600; macOS limits socket paths to 104 bytes and shared-memory names to 31 characters, which the library's random names fit (internal detail).
- Other GOOS compile, but `Listener.Accept` fails with an error for which `errors.Is(err, errors.ErrUnsupported)` is true, and `Dial` then returns `ErrRejected`.
- Windows' clock ticks in 0.5–15.6 ms steps, so `ipcbench` times latency with `QueryPerformanceCounter` and a Spin may last up to one timer tick before parking.
- Each direction of a lane serves one goroutine at a time; use more lanes for more concurrency.
- A lane is used either through the Lane API or through one `NewStream` on each end, never both.
- Spin busy-waits a CPU for up to Spin per blocking call; set it small or negative when CPUs are scarce.
- Connections are not persistent: once a closed peer's records are drained, nothing is replayed.
