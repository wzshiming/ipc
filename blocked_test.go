package ipc

import (
	"bytes"
	"runtime"
	"sync/atomic"
	"testing"
	"time"
)

// await waits until cond holds, yielding the processor meanwhile, and fails
// t if it does not within a minute.
func await(t *testing.T, what string, cond func() bool) {
	t.Helper()
	for give := time.Now().Add(time.Minute); !cond(); runtime.Gosched() {
		if time.Now().After(give) {
			t.Fatalf("waited a minute for %s", what)
		}
	}
}

// awaitReceiveParked waits until a receive on c parks in Next, as the mark
// Next sets in the header of the ring c reads shows.
func awaitReceiveParked(t *testing.T, c *Conn) {
	t.Helper()
	parked := consumerParked(c.seg.Bytes(), c.ringSize, 1-c.sendDir)
	await(t, "the receive to park", func() bool { return atomic.LoadUint32(parked) != 0 })
}

// awaitSendParked waits until a send on c parks in Reserve, as the mark
// Reserve sets in the header of the ring c fills shows.
func awaitSendParked(t *testing.T, c *Conn) {
	t.Helper()
	parked := producerParked(c.seg.Bytes(), c.ringSize, c.sendDir)
	await(t, "the send to park", func() bool { return atomic.LoadUint32(parked) != 0 })
}

// awaitQueued waits until a stream call waits for g behind the one holding it.
func awaitQueued(t *testing.T, g *gate) {
	t.Helper()
	await(t, "a stream call to queue", func() bool { return g.state.Load()&gateWaiters != 0 })
}

// goroutines counts the goroutines with frame on their stacks that, unless
// state is empty, wait in state, as runtime.Stack reports them.
func goroutines(state, frame string) int {
	buf := make([]byte, 64<<10)
	for {
		n := runtime.Stack(buf, true)
		if n < len(buf) {
			buf = buf[:n]
			break
		}
		buf = make([]byte, 2*len(buf))
	}
	n := 0
	for _, g := range bytes.Split(buf, []byte("\n\n")) {
		header, _, _ := bytes.Cut(g, []byte("\n"))
		waiting := state == "" || bytes.Contains(header, []byte("["+state+"]")) || bytes.Contains(header, []byte("["+state+","))
		if waiting && bytes.Contains(g, []byte(frame)) {
			n++
		}
	}
	return n
}
