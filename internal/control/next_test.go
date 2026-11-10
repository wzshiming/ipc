package control

import (
	"bytes"
	"errors"
	"io"
	"testing"
	"time"
)

// countingReader counts the reads a link makes on its steady reader.
type countingReader struct {
	io.Reader
	reads int
}

func (r *countingReader) Read(p []byte) (int, error) {
	r.reads++
	return r.Reader.Read(p)
}

// chunkReader cuts every read a link makes on its steady reader to n bytes.
type chunkReader struct {
	io.Reader
	n int
}

func (r *chunkReader) Read(p []byte) (int, error) {
	return r.Reader.Read(p[:min(len(p), r.n)])
}

func TestNextTakesQueuedFramesInOneRead(t *testing.T) {
	a, b := pair(t)
	r := &countingReader{Reader: b.r}
	b.r = r
	const n = 100
	for i := range n {
		if err := a.Send(4, []byte{byte(i)}, nil); err != nil {
			t.Fatal(err)
		}
	}
	for i := range n {
		typ, p, err := b.Next()
		if err != nil {
			t.Fatalf("frame %d: %v", i, err)
		}
		if typ != 4 || len(p) != 1 || p[0] != byte(i) {
			t.Fatalf("frame %d: typ=%d payload=%v", i, typ, p)
		}
	}
	if r.reads != 1 {
		t.Fatalf("%d queued frames took %d reads; want 1", n, r.reads)
	}
}

func TestNextContinuesFrameSplitAcrossReads(t *testing.T) {
	a, b := pair(t)
	b.r = &chunkReader{Reader: b.r, n: 4} // shorter than a header
	frames := [][]byte{nil, {1}, {2, 3, 4, 5, 6, 7, 8}, bytes.Repeat([]byte{9}, 100)}
	for i, p := range frames {
		if err := a.Send(uint8(i), p, nil); err != nil {
			t.Fatal(err)
		}
	}
	for i, want := range frames {
		typ, p, err := b.Next()
		if err != nil {
			t.Fatalf("frame %d: %v", i, err)
		}
		if typ != uint8(i) || !bytes.Equal(p, want) {
			t.Fatalf("frame %d: typ=%d payload=%v; want typ=%d payload=%v", i, typ, p, i, want)
		}
	}
}

func TestNextReceivesFramesLargerThanItsBuffer(t *testing.T) {
	a, b := pair(t)
	big := make([]byte, MaxPayload)
	for i := range big {
		big[i] = byte(i * 7)
	}
	errc := make(chan error, 1)
	go func() {
		for i := range 3 {
			if err := a.Send(uint8(i), big[:len(big)-i], nil); err != nil {
				errc <- err
				return
			}
			if err := a.Send(9, []byte{byte(i)}, nil); err != nil {
				errc <- err
				return
			}
		}
		errc <- nil
	}()
	for i := range 3 {
		typ, p, err := b.Next()
		if err != nil || typ != uint8(i) || !bytes.Equal(p, big[:len(big)-i]) {
			t.Fatalf("big frame %d: typ=%d len=%d err=%v", i, typ, len(p), err)
		}
		typ, p, err = b.Next()
		if err != nil || typ != 9 || len(p) != 1 || p[0] != byte(i) {
			t.Fatalf("small frame %d: typ=%d payload=%v err=%v", i, typ, p, err)
		}
	}
	if err := <-errc; err != nil {
		t.Fatal(err)
	}
}

func TestNextAllocatesNothingPerFrame(t *testing.T) {
	a, b := pair(t)
	allocs := testing.AllocsPerRun(100, func() {
		if err := a.Send(4, []byte{1}, nil); err != nil {
			t.Fatal(err)
		}
		if _, _, err := b.Next(); err != nil {
			t.Fatal(err)
		}
	})
	if allocs != 0 {
		t.Fatalf("Next allocates %v per frame; want 0", allocs)
	}
}

func TestNextReturnsEOFWhenPeerClosesBetweenFrames(t *testing.T) {
	a, b := pair(t)
	if err := a.Send(1, []byte("x"), nil); err != nil {
		t.Fatal(err)
	}
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	if typ, p, err := b.Next(); err != nil || typ != 1 || string(p) != "x" {
		t.Fatalf("last frame: typ=%d payload=%q err=%v", typ, p, err)
	}
	if _, _, err := b.Next(); err != io.EOF {
		t.Fatalf("got %v, want io.EOF", err)
	}
}

func TestNextReturnsUnexpectedEOFWhenPeerClosesMidFrame(t *testing.T) {
	a, b := pair(t)
	if _, err := a.c.Write([]byte{1, 0, 0}); err != nil { // a header cut short
		t.Fatal(err)
	}
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := b.Next(); err != io.ErrUnexpectedEOF {
		t.Fatalf("got %v, want io.ErrUnexpectedEOF", err)
	}
}

func TestCloseWakesBlockedNext(t *testing.T) {
	_, b := pair(t)
	errc := make(chan error, 1)
	go func() {
		_, _, err := b.Next()
		errc <- err
	}()
	time.Sleep(50 * time.Millisecond)
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-errc:
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("got %v, want ErrClosed", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not wake Next")
	}
}
