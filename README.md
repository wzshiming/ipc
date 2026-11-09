# ipc

`github.com/wzshiming/ipc` is a pure-Go (no cgo), cross-platform (Linux, macOS, Windows) shared-memory IPC library for two processes on one machine. A connection is a set of independent lanes, each a pair of single-producer/single-consumer rings in one shared-memory segment; records are written and read in place, so the data path makes no system calls and no copies beyond the producer filling its record. An AF_UNIX socket carries only the handshake, the shared-memory handle, doorbells that wake a parked peer, and goodbye. Throughput scales by adding lanes; the design target is 1 Tbps aggregate.

## Install

```shell
go get github.com/wzshiming/ipc
```

## Packages

### `github.com/wzshiming/ipc`

```go
// Config sizes a connection: limits and defaults on a Listener, a request on Dial.
type Config struct {
	Lanes    int           // independent bidirectional lanes; 0 means 1
	RingSize int           // ring bytes per lane direction, a power of two in [64 KiB, 1 GiB]; 0 means 4 MiB
	Spin     time.Duration // busy re-check time before parking; 0 means 50 µs, negative parks at once
}

// Listen listens on the AF_UNIX socket at path.
func Listen(path string, cfg Config) (*Listener, error)
// Accept waits for a dialer and completes its handshake.
func (l *Listener) Accept(ctx context.Context) (*Conn, error)
// Close stops listening; connections already accepted stay open.
func (l *Listener) Close() error
// Addr returns the listener's socket path.
func (l *Listener) Addr() net.Addr

// Dial connects to the listener at path; the listener clamps cfg to its own limits.
func Dial(ctx context.Context, path string, cfg Config) (*Conn, error)

// Lanes returns the number of lanes.
func (c *Conn) Lanes() int
// Lane returns lane i, 0 <= i < Lanes().
func (c *Conn) Lane(i int) *Lane
// RingSize returns each lane's ring capacity per direction.
func (c *Conn) RingSize() int
// MaxRecord returns the largest record a lane accepts: RingSize()/2 - 8.
func (c *Conn) MaxRecord() int
// LocalAddr and RemoteAddr return the socket path the connection was made on.
func (c *Conn) LocalAddr() net.Addr
func (c *Conn) RemoteAddr() net.Addr
// Close says goodbye and wakes every blocked call with ErrClosed (ErrPeerClosed on the peer,
// whose receives still drain the records committed before Close).
func (c *Conn) Close() error
// Done is closed once the connection is closed by either end or fails.
func (c *Conn) Done() <-chan struct{}
// Err returns nil while open, then why the connection closed.
func (c *Conn) Err() error

// Conn returns the connection the lane belongs to.
func (l *Lane) Conn() *Conn
// Index returns the lane's index in its connection.
func (l *Lane) Index() int
// TryReserve returns an n-byte slice of the ring to fill, or ErrWouldBlock.
func (l *Lane) TryReserve(n int) ([]byte, error)
// Reserve is TryReserve that waits for room (spin, then park).
func (l *Lane) Reserve(ctx context.Context, n int) ([]byte, error)
// Commit publishes the reserved record.
func (l *Lane) Commit() error
// Abort discards the reserved record, if any.
func (l *Lane) Abort()
// TryNext returns the next record in place, or ErrWouldBlock.
func (l *Lane) TryNext() ([]byte, error)
// Next is TryNext that waits for a record (spin, then park).
func (l *Lane) Next(ctx context.Context) ([]byte, error)
// Release frees the record returned by Next or TryNext.
func (l *Lane) Release() error
// Send copies p into one record.
func (l *Lane) Send(ctx context.Context, p []byte) error
// Recv copies the next record into p; a longer record yields io.ErrShortBuffer.
func (l *Lane) Recv(ctx context.Context, p []byte) (int, error)

// NewStream presents a lane as a net.Conn byte stream; Close sends EOF.
func NewStream(l *Lane) net.Conn

var (
	ErrClosed     // this end closed the connection or listener
	ErrPeerClosed // the peer closed the connection or died
	ErrWouldBlock // TryReserve on a full lane, TryNext on an empty one
	ErrTooLarge   // record longer than MaxRecord
	ErrCorrupt    // shared memory violates the ring format
	ErrRejected   // Dial: the listener refused the handshake
)
```

A Reserve slice is valid until Commit or Abort, a Next slice until Release; both alias shared memory.

### `github.com/wzshiming/ipc/cmd/ipcbench`

A cross-process benchmark: the process you start listens and consumes, and re-executes itself to dial and produce, so every rate is measured on the consumer side after Release. Exit code 0 on success, 1 on error or when nothing was measured, 2 on a payload integrity failure.

```shell
go run ./cmd/ipcbench [flags]
```

| Flag | Default | Meaning |
| --- | --- | --- |
| `-mode` | `throughput` | `throughput` (bytes/s of large records), `framing` (records/s of small records) or `latency` (64 B ping-pong round trips, one lane) |
| `-lanes` | `1` | number of lanes, each with its own producer and consumer thread |
| `-size` | `1MiB` for throughput, `64B` otherwise | record size; `B`, `KiB`, `MiB`, `GiB` suffixes allowed |
| `-ring` | `4MiB`, or the power of two holding 4 records | ring size per lane direction |
| `-duration` | `5s` | how long the peer produces |
| `-spin` | `0` (library default) | poll time before parking; negative parks at once |
| `-json` | `false` | print the report as JSON |
| `-path` | `b.sock` in a new temporary directory | control socket path |

`-peer` and `-corrupt` are internal flags used by the re-executed peer and by tests.

## Example

The example below is [example_test.go](example_test.go); `go test -run Example .` compiles and runs it. The listener and the dialer would normally be two processes.

```go
package ipc_test

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"

	"github.com/wzshiming/ipc"
)

// connect listens on a fresh socket and returns both ends of one connection.
// In real use the listener and the dialer are two processes.
func connect(ctx context.Context, cfg ipc.Config) (server, client *ipc.Conn, cleanup func()) {
	dir, err := os.MkdirTemp("", "ipc") // keep the path short: sun_path is ≤ 104 bytes on macOS
	if err != nil {
		log.Fatal(err)
	}
	path := filepath.Join(dir, "s")

	l, err := ipc.Listen(path, cfg)
	if err != nil {
		log.Fatal(err)
	}
	accepted := make(chan *ipc.Conn, 1)
	go func() {
		c, err := l.Accept(ctx)
		if err != nil {
			log.Fatal(err)
		}
		accepted <- c
	}()

	client, err = ipc.Dial(ctx, path, cfg)
	if err != nil {
		log.Fatal(err)
	}
	server = <-accepted
	return server, client, func() {
		client.Close()
		server.Close()
		l.Close()
		os.RemoveAll(dir)
	}
}

// Records are written and read in place in shared memory.
func Example() {
	ctx := context.Background()
	server, client, cleanup := connect(ctx, ipc.Config{Lanes: 2, RingSize: 64 << 10})
	defer cleanup()
	fmt.Println("lanes:", client.Lanes(), "max record:", client.MaxRecord())

	tx := client.Lane(1)
	p, err := tx.Reserve(ctx, len("hello"))
	if err != nil {
		log.Fatal(err)
	}
	copy(p, "hello") // fill the record where the peer will read it
	if err := tx.Commit(); err != nil {
		log.Fatal(err)
	}

	rx := server.Lane(1)
	rec, err := rx.Next(ctx)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("lane %d got %q\n", rx.Index(), rec) // rec aliases the ring until Release
	if err := rx.Release(); err != nil {
		log.Fatal(err)
	}
	// Output:
	// lanes: 2 max record: 32760
	// lane 1 got "hello"
}

// NewStream turns a lane into a net.Conn byte stream.
func ExampleNewStream() {
	ctx := context.Background()
	server, client, cleanup := connect(ctx, ipc.Config{})
	defer cleanup()

	w := ipc.NewStream(client.Lane(0))
	if _, err := io.WriteString(w, "hello, stream"); err != nil {
		log.Fatal(err)
	}
	w.Close() // the peer's Read sees io.EOF

	r := ipc.NewStream(server.Lane(0))
	defer r.Close()
	b, err := io.ReadAll(r)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(string(b))
	// Output:
	// hello, stream
}
```

## Design

### Ring layout and record format

Each ring is a 4096-byte header followed by its data region:

| Offset | Field |
| --- | --- |
| 0 | magic `RING` (u32), version 1 (u32), capacity (u64) |
| 128 | head: bytes committed by the producer |
| 256 | tail: bytes released by the consumer |
| 384 | consumerParked flag |
| 512 | producerParked flag |
| 4096 | data region, capacity bytes |

Every field sits on its own 128-byte line so producer and consumer never share a cache line. The capacity is a power of two in [64 KiB, 1 GiB]. A record is an 8-byte header (u32 length, u32 flags, one of which is the wrap marker) followed by the payload padded to 8 bytes. Records are never split across the end of the region: a record that does not fit before the end leaves a wrap marker and starts again at offset 0. Hence `MaxRecord = RingSize/2 − 8`.

### Segment creation and hand-off

One shared-memory segment per connection, created by the listener, holds 2 × Lanes rings and is handed to the dialer over the control socket:

| Platform | Segment | Hand-off |
| --- | --- | --- |
| Linux | `memfd_create` | fd over the AF_UNIX control socket with `SCM_RIGHTS` |
| macOS | `shm_open` with a random name, `shm_unlink`ed immediately (libc called through a cgo-free trampoline) | fd with `SCM_RIGHTS` |
| Windows | named `CreateFileMapping` in the `Local\` namespace with a random 128-bit name | the name over the control socket |

On Linux and macOS no name outlives the hand-off; the memory is freed when both ends have unmapped it.

### Parking and doorbells

A blocking `Reserve` or `Next` that cannot proceed:

1. spins for `Config.Spin`, busy re-checking the ring without yielding its thread;
2. sets its parked flag with an atomic swap and re-checks the ring;
3. blocks on a doorbell frame from the control socket.

The committing (or releasing) side swaps the peer's parked flag back and sends one doorbell frame only if the peer was parked. Both sides swap the same flag word, so no wakeup is lost even under plain acquire/release atomics such as the race detector's. An uncontended ring therefore costs no system call.

### Lanes

Lane *i* uses ring 2*i* (listener → dialer) and ring 2*i*+1 (dialer → listener). Each direction of a lane is single-producer/single-consumer and serves one goroutine at a time; throughput is aggregated by adding lanes, which share nothing but the segment.

### Control protocol v1

```text
dialer   -> Hello{version, lanes, ringSize}
listener -> Reject{reason} | Welcome{lanes, ringSize, token}   (token: fd or segment name)
either   -> Doorbell{lane, dir}   (wake a parked peer)
either   -> Bye                   (orderly close)
```

The listener clamps the requested lanes and ring size to its own `Config`; both ends then use the granted values.

## Performance

Measured on 2026-11-09 on an Apple M1 Max (10 cores: 8 performance + 2 efficiency), macOS, Go 1.27.3, from a clean checkout, two 5-second runs each; GB = 10⁹ bytes. Producer and consumer goroutines are pinned with `runtime.LockOSThread`. In throughput mode the producer fills every byte of each record from a template via `copy` into the Reserve slice (the single copy), the consumer reads one word every 128 bytes of the Next slice to check the sequence number, and only bytes released by the consumer count.

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

In-process reference numbers from `go test -bench`: `internal/ring` Fill64KiB ≈ 43–49 GB/s on a single lane and Protocol64B 76–100 M records/s; the root package's `BenchmarkPinnedPingPong64B` ≈ 0.40 µs/op.

```shell
go test -run '^$' -bench . ./internal/ring
go test -run '^$' -bench PinnedPingPong64B .
```

**Towards 1 Tbps (a design target, not a measurement).** 1 Tbps is 125 GB/s. The M1 Max reaches ≈ 0.6 Tbps with 4 lanes and plateaus there, because each lane needs two pinned cores and the part has 8 performance cores. At the ≈ 18 GB/s per lane observed with 4 lanes, about 7 lanes would be needed, on a machine with at least 14 performance cores and roughly 250 GB/s of memory/fabric bandwidth: the consumer touches every 128-byte line, so memory traffic is about twice the payload unless the rings stay cache-resident (the default rings for 1 MiB records, 8 MiB, partly do here). The library adds no per-byte work beyond the producer's single copy.

CI only smoke-tests `ipcbench` (1-second runs on shared runners, no performance assertion).

## Platform notes

- **Windows** needs Windows 10 1803 or later for AF_UNIX sockets. Go's clock there ticks in 0.5–15.6 ms steps, so `ipcbench` times latency samples with `QueryPerformanceCounter`, and the same coarse clock bounds `Config.Spin`: a spin may last up to one timer tick before parking.
- **Linux and macOS** pass the segment as a file descriptor; fd passing is used only there. The socket file is chmod 0600.
- **macOS** shared-memory names are limited to 31 characters; the library's random names fit (an internal detail). Socket paths are limited to 104 bytes, so keep them short.
- **Other GOOS** compile, but creating or attaching a segment fails with an error for which `errors.Is(err, errors.ErrUnsupported)` is true: `Listener.Accept` returns it, and `Dial` returns `ErrRejected`.

## Limitations

- Each direction of a lane serves one goroutine at a time; use more lanes for more concurrency.
- A record is at most `MaxRecord = RingSize/2 − 8` bytes.
- Connections are not persistent: a crashed or exiting peer ends the connection with `ErrPeerClosed` once the records it committed are drained, and nothing is replayed.
- A lane is used either through the Lane API or through one `NewStream` on each end, never both.
- `Spin` busy-waits a CPU for up to `Spin` per blocking call; set it small or negative when CPUs are scarce.
- Small-record rates depend on spacing: with a tight spin, producer and consumer run one record apart and the head index crosses cores per record (≈ 23 M records/s at 64 B above); parking batches records and raises the rate (≈ 59 M records/s).
