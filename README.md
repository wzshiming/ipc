# ipc

Pure-Go (no cgo) shared-memory IPC between two processes on one machine, for Linux, macOS and Windows.

## Example

```go
l, _ := ipc.Listen(path) // path: an AF_UNIX socket path, ≤ 103 bytes on macOS
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

## Performance

`io.Copy` throughput at one connection with a 64 KiB record size, and the p50 of a 64 B round trip, from the CI bench job of master run [42808714536](https://github.com/wzshiming/ipc/actions/runs/42808714536) (GB = 10⁹ bytes):

| Runner | CPU | ipc GB/s | AF_UNIX GB/s | TCP GB/s | ipc p50 | AF_UNIX p50 | TCP p50 |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| ubuntu-latest | AMD EPYC 7763 64-Core Processor, 4 CPUs | 21.58 | 4.74 | 1.91 | 0.35 µs | 52.18 µs | 76.33 µs |
| macos-latest | Apple M1 (Virtual), 3 CPUs | 15.29 | 0.13 | 2.11 | 0.29 µs | 44.92 µs | 93.54 µs |
| windows-latest | AMD EPYC 7763 64-Core Processor, 4 CPUs | 21.07 | 2.46 | 2.14 | 0.40 µs | 71.40 µs | 77.60 µs |

`io.Copy` moves ipc's records in place but copies AF_UNIX and TCP through its 32 KiB buffer, and ipc spins before parking while the kernel transports block in Go's netpoller. Each bench job's summary has the full comparison; `go run ./cmd/ipcbench -mode compare -duration 1s` prints it locally.

## Platform notes

- Windows needs Windows 10 1803 or later for AF_UNIX sockets. Other GOOS, android and ios aside, compile without shared memory: `Accept` fails with `errors.ErrUnsupported` and `Dial` with `ErrRejected`, unless `Listen` and `Dial` fail first, as on plan9.
- The socket file is mode 0600 on Linux and macOS.
- Windows' clock ticks in 0.5–15.6 ms steps, so a spin may last up to one tick.

## License

Licensed under the MIT License. See [LICENSE](https://github.com/wzshiming/ipc/blob/master/LICENSE) for the full license text.
