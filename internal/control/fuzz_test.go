package control

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"testing"
	"time"
)

// recvAllocSlack leaves room for what the rest of the process allocates while
// Recv runs; Recv itself allocates only the payload.
const recvAllocSlack = 4 << 10

// frame returns a frame of type typ carrying payload, in the wire format.
func frame(typ uint8, payload []byte) []byte {
	return append(binary.LittleEndian.AppendUint32([]byte{typ}, uint32(len(payload))), payload...)
}

// splitFrames returns the frames stream holds whole and the errors Recv may
// end with after them.
func splitFrames(stream []byte) (frames [][]byte, ends []error) {
	for len(stream) > 0 {
		if len(stream) < headerSize {
			return frames, []error{io.EOF, io.ErrUnexpectedEOF}
		}
		n := binary.LittleEndian.Uint32(stream[1:])
		if n > MaxPayload {
			return frames, []error{ErrPayloadTooLarge}
		}
		if len(stream)-headerSize < int(n) {
			return frames, []error{io.EOF, io.ErrUnexpectedEOF}
		}
		frames, stream = append(frames, stream[:headerSize+n]), stream[headerSize+n:]
	}
	return frames, []error{io.EOF}
}

// FuzzRecv checks that Recv of what a raw peer writes before shutting its side
// never panics, allocates at most MaxPayload for a frame, and returns the
// stream's whole frames, then io.EOF at a frame boundary, io.EOF or
// io.ErrUnexpectedEOF in a cut frame, ErrPayloadTooLarge at a longer length.
func FuzzRecv(f *testing.F) {
	dir, err := os.MkdirTemp("", "ipcfuzz")
	if err != nil {
		f.Fatal(err)
	}
	f.Cleanup(func() { os.RemoveAll(dir) })
	path := filepath.Join(dir, "s.sock")
	l, err := Listen(context.Background(), path)
	if errors.Is(err, ErrUnsupported) {
		f.Skipf("AF_UNIX unsupported: %v", err)
	}
	if err != nil {
		f.Fatal(err)
	}
	f.Cleanup(func() { l.Close() })

	f.Add([]byte(nil))
	f.Add(append(frame(4, []byte{1}), frame(5, nil)...))
	f.Add(binary.LittleEndian.AppendUint32([]byte{1}, MaxPayload))   // the largest payload, cut
	f.Add(binary.LittleEndian.AppendUint32([]byte{1}, MaxPayload+1)) // a length above MaxPayload
	f.Add(frame(1, []byte("payload"))[:headerSize-2])                // a cut header
	f.Add(frame(1, []byte("payload"))[:headerSize+3])                // a cut payload
	f.Fuzz(func(t *testing.T, stream []byte) {
		raw, err := net.Dial("unix", path)
		if err != nil {
			t.Fatal(err)
		}
		defer raw.Close()
		k, err := l.Accept()
		if err != nil {
			t.Fatal(err)
		}
		wrote := make(chan struct{})
		go func() {
			defer close(wrote)
			if _, err := raw.Write(stream); err == nil {
				raw.(*net.UnixConn).CloseWrite()
			}
		}()
		defer func() { k.Close(); <-wrote }()
		// A Recv that blocks past the stream's end fails instead of hanging.
		k.SetReadDeadline(time.Now().Add(10 * time.Second))

		frames, ends := splitFrames(stream)
		var before, after runtime.MemStats
		for i := 0; i <= len(frames); i++ {
			runtime.ReadMemStats(&before)
			typ, p, fds, err := k.Recv()
			runtime.ReadMemStats(&after)
			if n := after.TotalAlloc - before.TotalAlloc; n > MaxPayload+recvAllocSlack {
				t.Fatalf("Recv %d allocated %d bytes; want at most MaxPayload, %d", i, n, MaxPayload)
			}
			if i < len(frames) {
				if w := frames[i]; err != nil || typ != w[0] || !bytes.Equal(p, w[headerSize:]) || len(fds) != 0 {
					t.Fatalf("Recv %d = %d %x %v, %v; want the frame %x", i, typ, p, fds, err, w)
				}
				continue
			}
			if !slices.ContainsFunc(ends, func(end error) bool { return errors.Is(err, end) }) {
				t.Fatalf("Recv after the %d whole frames = %d %x, %v; want one of %v", i, typ, p, err, ends)
			}
		}
	})
}
