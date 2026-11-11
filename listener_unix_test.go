//go:build darwin || linux

package ipc

import (
	"encoding/binary"
	"net"
	"testing"
	"time"

	"golang.org/x/sys/unix"

	"github.com/wzshiming/ipc/internal/ring"
)

// damageOnWelcome plays a dialer that asks for a ring of ringSize and, as
// soon as the Welcome's descriptor arrives, misaligns the tail of the ring
// the listener reads, at offset 256 of its header, then hangs up. It polls
// its socket instead of parking, so that it strikes while the listener may
// still be setting up its end.
func damageOnWelcome(t *testing.T, path string, ringSize int) {
	t.Helper()
	fd, err := unix.Socket(unix.AF_UNIX, unix.SOCK_STREAM, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer unix.Close(fd)
	if err := unix.Connect(fd, &unix.SockaddrUnix{Name: path}); err != nil {
		t.Fatal(err)
	}
	h := hello{version: protoVersion, ringSize: uint64(ringSize)}.encode()
	frame := append(binary.LittleEndian.AppendUint32([]byte{frameHello}, uint32(len(h))), h...)
	if _, err := unix.Write(fd, frame); err != nil {
		t.Fatal(err)
	}
	buf, oob := make([]byte, 64), make([]byte, unix.CmsgSpace(4))
	for give := time.Now().Add(2 * time.Second); time.Now().Before(give); {
		_, oobn, _, _, err := unix.Recvmsg(fd, buf, oob, unix.MSG_DONTWAIT)
		if err == unix.EAGAIN {
			continue
		}
		if err != nil || oobn == 0 {
			return
		}
		msgs, err := unix.ParseSocketControlMessage(oob[:oobn])
		if err != nil || len(msgs) != 1 {
			return
		}
		fds, err := unix.ParseUnixRights(&msgs[0])
		if err != nil {
			return
		}
		defer func() {
			for _, fd := range fds {
				unix.Close(fd)
			}
		}()
		rs := ring.Size(ringSize)
		mem, err := unix.Mmap(fds[0], 0, 2*rs, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
		if err != nil {
			return
		}
		mem[int(dirDialerToListener)*rs+256] = 1
		unix.Munmap(mem)
		return
	}
}

func TestDialersDamagingTheRingHeadersOnTheWelcomeCannotFailAccept(t *testing.T) {
	const ringSize = 64 << 10
	l, path := listen(t, ListenConfig{RingSize: ringSize})
	accepted := make(chan net.Conn, 256)
	failed := make(chan error, 1)
	go func() {
		defer close(accepted)
		for {
			c, err := l.Accept()
			if err != nil {
				failed <- err
				return
			}
			accepted <- c
		}
	}()
	t.Cleanup(func() {
		l.Close()
		for c := range accepted {
			c.Close()
		}
	})
	for i := range 200 {
		select {
		case err := <-failed:
			t.Fatalf("Accept = %v after %d dialers that damage the ring headers on the Welcome; want it never to fail", err, i)
		default:
		}
		damageOnWelcome(t, path, ringSize)
	}
	dc, err := (&Dialer{RingSize: ringSize, Timeout: 10 * time.Second}).Dial(path)
	if err != nil {
		t.Fatal("Dial after the damaging dialers:", err)
	}
	defer dc.Close()
	if _, err := dc.Write([]byte("honest")); err != nil {
		t.Fatal(err)
	}
	// Accept serves the honest dialer: one connection reads what it wrote.
	hang := time.After(time.Minute)
	buf := make([]byte, 16)
	for {
		select {
		case err := <-failed:
			t.Fatalf("Accept = %v after the damaging dialers; want it to serve the honest one", err)
		case c := <-accepted:
			c.SetReadDeadline(time.Now().Add(10 * time.Second))
			n, err := c.Read(buf)
			c.Close()
			if err == nil && string(buf[:n]) == "honest" {
				return
			}
		case <-hang:
			t.Fatal("Accept did not return the honest dialer's connection")
		}
	}
}
