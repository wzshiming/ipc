// Package ring implements a single-producer single-consumer ring of
// variable-length records over a caller-provided byte region, normally a
// shared-memory segment mapped by two processes.
//
// # Layout
//
// The region is a HeaderSize (4096-byte) header followed by a data area of
// capacity bytes, a power of two in [64 KiB, 1 GiB]. In the header,
// magic/version/capacity, the producer cursor head, the consumer cursor tail,
// consumerParked and producerParked each own a 128-byte-aligned cache line.
// Cursors are monotonically increasing uint64 positions; the data offset of a
// position is pos & (capacity-1).
//
// A record is an 8-byte header (u32 length, u32 flags) followed by its
// payload, padded to 8 bytes. When a record does not fit before the end of
// the data area the producer writes a header with the wrap flag and the
// record starts at offset 0, so every payload is contiguous. Because records
// are at most MaxRecord = capacity/2 - 8 bytes, the skipped bytes plus the
// record never exceed capacity.
//
// # Memory model
//
// Every shared header field is accessed only through sync/atomic. The ring
// relies on acquire/release ordering only, never on sequential consistency:
// under -race the runtime implements atomic loads as acquire and stores as
// release, and across processes on shared memory nothing restores seq_cst,
// so a store followed by a load of another word may be reordered (see
// Parking). The producer writes the payload and record header with plain
// stores, then publishes them by storing head; the consumer loads head before
// reading them. Symmetrically the consumer stores tail after it is done with a
// payload, and the producer loads tail before reusing those bytes. The race
// detector sees these atomic pairs as the happens-before edges ordering
// payload accesses.
//
// Each side keeps a process-local copy of the other side's cursor and reloads
// it only when the copy says the ring is full (producer) or empty (consumer).
//
// The consumer trusts nothing in the data area or in head: lengths are checked
// against MaxRecord, the published bytes and the end of the data area, and a
// violation yields ErrCorrupt rather than a panic or an out-of-range slice.
//
// # Parking
//
// A side that cannot make progress calls Park, which swaps its parked flag to
// 1 and then re-checks the ring. The other side, after publishing its cursor,
// unconditionally swaps that flag to 0 and rings a doorbell if it was set.
// Both sides read-modify-write the same flag word, so their accesses are
// coherence-ordered: either the publisher's swap reads the parker's 1, or the
// parker's acquire swap reads the publisher's release swap, synchronizes with
// it and its re-check sees the publish. No wakeup is lost even under
// acquire/release atomics, which is what the race runtime provides; a plain
// store followed by a load would give no such guarantee there. Spurious
// wakeups are possible. A flag is cleared by exactly one swap, so each park
// yields at most one doorbell, and Unpark clears it so an abandoned wait is
// not woken (a stale clear costs at most a spurious doorbell).
//
// # Concurrency
//
// Each ring has at most one Producer and one Consumer across all processes,
// and each is used by a single goroutine.
//
// # Alignment
//
// The caller guarantees the region base is at least 8-byte aligned; all
// 64-bit atomic fields sit at 128-byte-aligned offsets, so 64-bit atomics are
// valid on 32-bit platforms too.
package ring
