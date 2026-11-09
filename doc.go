// Package ipc is a pure-Go, cross-platform shared-memory IPC library built on
// zero-copy SPSC rings in a shared segment with an AF_UNIX control link.
//
// Package layout:
//
//   - ipc: the public API (this package).
//   - internal/ring: the SPSC zero-copy ring.
//   - internal/shm: the shared-memory segment.
//   - internal/control: the AF_UNIX control link.
//   - cmd/ipcbench: the benchmark tool.
package ipc
