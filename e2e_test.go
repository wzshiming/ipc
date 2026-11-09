package ipc

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"math/rand/v2"
	"os"
	"os/exec"
	"strconv"
	"sync"
	"testing"
	"time"
)

// The test binary re-executes itself as the dialing peer; these variables
// select the child's role and pass it the socket path and stream volume.
const (
	envRole = "IPC_E2E_ROLE"
	envPath = "IPC_E2E_PATH"
	// envMiB sets the stream volume per lane per direction in MiB (default 256).
	envMiB = "IPC_E2E_MIB"
)

const (
	e2eLanes      = 4
	pingPongCount = 1_000_000
	recHdr        = 12 // seq u64 | crc32 u32
	maxBody       = 64 << 10
)

func TestMain(m *testing.M) {
	if role := os.Getenv(envRole); role != "" {
		if err := runChild(role, os.Getenv(envPath)); err != nil {
			fmt.Fprintln(os.Stderr, "child:", err)
			os.Exit(1)
		}
		os.Exit(0)
	}
	os.Exit(m.Run())
}

func runChild(role, path string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	cfg := Config{Lanes: 1}
	if role == "stream" {
		cfg.Lanes = e2eLanes
	}
	c, err := Dial(ctx, path, cfg)
	cancel()
	if err != nil {
		return err
	}
	defer c.Close()
	switch role {
	case "stream":
		return runStream(c, 1, streamVolume())
	case "echo":
		l := c.Lane(0)
		for {
			rec, err := l.Next(context.Background())
			if errors.Is(err, ErrPeerClosed) {
				return nil
			}
			if err != nil {
				return err
			}
			p, err := l.Reserve(context.Background(), len(rec))
			if err != nil {
				return err
			}
			copy(p, rec)
			if err := l.Commit(); err != nil {
				return err
			}
			if err := l.Release(); err != nil {
				return err
			}
		}
	case "hang":
		_, err := c.Lane(0).Next(context.Background())
		return err
	}
	return fmt.Errorf("unknown role %q", role)
}

func streamVolume() int64 {
	mib := int64(256)
	if s := os.Getenv(envMiB); s != "" {
		if v, err := strconv.ParseInt(s, 10, 64); err == nil && v > 0 {
			mib = v
		}
	}
	return mib << 20
}

var pattern = func() []byte {
	p := make([]byte, 1<<20+maxBody)
	r := rand.New(rand.NewPCG(1, 2))
	for i := 0; i < len(p); i += 8 {
		binary.LittleEndian.PutUint64(p[i:], r.Uint64())
	}
	return p
}()

// runStream sends volume bytes of variable-length records on every lane in
// direction dir (0: listener→dialer, 1: dialer→listener) and checks every
// record arriving from the other direction by sequence number and CRC.
func runStream(c *Conn, dir uint64, volume int64) error {
	var wg sync.WaitGroup
	errs := make(chan error, 2*c.Lanes())
	for i := range c.Lanes() {
		l := c.Lane(i)
		wg.Add(2)
		go func() {
			defer wg.Done()
			if err := sendStream(l, dir, volume); err != nil {
				errs <- fmt.Errorf("lane %d send: %w", l.Index(), err)
			}
		}()
		go func() {
			defer wg.Done()
			if err := recvStream(l, volume); err != nil {
				errs <- fmt.Errorf("lane %d recv: %w", l.Index(), err)
			}
		}()
	}
	wg.Wait()
	close(errs)
	return <-errs
}

func sendStream(l *Lane, dir uint64, volume int64) error {
	ctx := context.Background()
	r := rand.New(rand.NewPCG(uint64(l.Index()), dir))
	var seq uint64
	for sent := int64(0); sent < volume; seq++ {
		body := r.IntN(maxBody + 1)
		off := int(seq*4099) % (len(pattern) - maxBody)
		p, err := l.Reserve(ctx, recHdr+body)
		if err != nil {
			return err
		}
		b := pattern[off : off+body]
		binary.LittleEndian.PutUint64(p, seq)
		binary.LittleEndian.PutUint32(p[8:], crc32.ChecksumIEEE(b))
		copy(p[recHdr:], b)
		if err := l.Commit(); err != nil {
			return err
		}
		sent += int64(recHdr + body)
	}
	// A header-less record marks the end of the stream.
	if _, err := l.Reserve(ctx, 0); err != nil {
		return err
	}
	return l.Commit()
}

func recvStream(l *Lane, volume int64) error {
	ctx := context.Background()
	var got int64
	for seq := uint64(0); ; seq++ {
		rec, err := l.Next(ctx)
		if err != nil {
			return err
		}
		if len(rec) == 0 {
			l.Release()
			if got < volume {
				return fmt.Errorf("stream ended after %d bytes; want %d", got, volume)
			}
			return nil
		}
		if len(rec) < recHdr {
			return fmt.Errorf("record %d is %d bytes", seq, len(rec))
		}
		if s := binary.LittleEndian.Uint64(rec); s != seq {
			return fmt.Errorf("record seq %d; want %d", s, seq)
		}
		if sum := crc32.ChecksumIEEE(rec[recHdr:]); sum != binary.LittleEndian.Uint32(rec[8:]) {
			return fmt.Errorf("record %d: crc mismatch", seq)
		}
		got += int64(len(rec))
		if err := l.Release(); err != nil {
			return err
		}
	}
}

// startPeer listens, starts the child in role and returns the accepted Conn.
func startPeer(t *testing.T, role string, cfg Config) (*Conn, *exec.Cmd) {
	t.Helper()
	l, path := listen(t, cfg)
	cmd := exec.Command(os.Args[0], "-test.run=^$")
	cmd.Env = append(os.Environ(), envRole+"="+role, envPath+"="+path)
	cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cmd.Process.Kill()
		cmd.Wait()
	})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	c, err := l.Accept(ctx)
	if err != nil {
		t.Fatal("Accept:", err)
	}
	t.Cleanup(func() { c.Close() })
	return c, cmd
}

func TestE2EFourLanesBidirectionalStream(t *testing.T) {
	c, cmd := startPeer(t, "stream", Config{Lanes: e2eLanes})
	if c.Lanes() != e2eLanes {
		t.Fatalf("Lanes = %d; want %d", c.Lanes(), e2eLanes)
	}
	volume := streamVolume()
	start := time.Now()
	if err := runStream(c, 0, volume); err != nil {
		t.Fatal(err)
	}
	if err := cmd.Wait(); err != nil {
		t.Fatal("child:", err)
	}
	d := time.Since(start)
	total := float64(2 * e2eLanes * volume)
	t.Logf("%d lanes x 2 directions x %d MiB in %v: %.2f GB/s aggregate", e2eLanes, volume>>20, d, total/d.Seconds()/1e9)
}

func TestE2EPingPong64B(t *testing.T) {
	c, cmd := startPeer(t, "echo", Config{})
	l := c.Lane(0)
	ctx := context.Background()
	start := time.Now()
	for seq := uint64(0); seq < pingPongCount; seq++ {
		p, err := l.Reserve(ctx, 64)
		if err != nil {
			t.Fatal(err)
		}
		binary.LittleEndian.PutUint64(p, seq)
		if err := l.Commit(); err != nil {
			t.Fatal(err)
		}
		rec, err := l.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if len(rec) != 64 || binary.LittleEndian.Uint64(rec) != seq {
			t.Fatalf("echo %d: got %d bytes seq %d", seq, len(rec), binary.LittleEndian.Uint64(rec))
		}
		if err := l.Release(); err != nil {
			t.Fatal(err)
		}
	}
	d := time.Since(start)
	c.Close()
	if err := cmd.Wait(); err != nil {
		t.Fatal("child:", err)
	}
	t.Logf("%d round trips of 64 B in %v (%v each)", pingPongCount, d, d/pingPongCount)
}

func TestE2EKilledPeerWakesBlockedNextWithErrPeerClosed(t *testing.T) {
	c, cmd := startPeer(t, "hang", Config{Spin: -1})
	ch := make(chan error, 1)
	go func() {
		_, err := c.Lane(0).Next(context.Background())
		ch <- err
	}()
	time.Sleep(50 * time.Millisecond)
	if err := cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	if err := waitErr(t, ch, time.Second); !errors.Is(err, ErrPeerClosed) {
		t.Fatalf("Next = %v; want ErrPeerClosed", err)
	}
}
