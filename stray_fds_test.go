//go:build darwin || linux

package ipc

import (
	"errors"
	"io"
	"os"
	"testing"
	"time"

	"github.com/wzshiming/ipc/internal/control"
)

func answerHello(t *testing.T, typ uint8, fd int) error {
	path := sockPath(t)
	cl, err := control.Listen(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	go func() {
		link, err := cl.Accept()
		if err != nil {
			return
		}
		defer link.Close()
		if _, _, _, err := link.Recv(); err == nil {
			link.Send(typ, nil, []int{fd})
			link.Recv()
		}
	}()
	c, err := Dial(path)
	if err == nil {
		c.Close()
	}
	return err
}

func TestStrayDescriptorsAreClosed(t *testing.T) {
	for name, deliver := range map[string]func(t *testing.T, fd int){
		"Hello to the listener": func(t *testing.T, fd int) {
			l, path := listen(t, ListenConfig{})
			acceptOne(t, l)
			link, err := control.Dial(t.Context(), path)
			if err != nil {
				t.Fatal(err)
			}
			defer link.Close()
			if err := link.Send(frameHello, hello{ringSize: 64 << 10}.encode(), []int{fd}); err != nil {
				t.Fatal(err)
			}
		},
		"Reject to the dialer": func(t *testing.T, fd int) {
			if err := answerHello(t, frameReject, fd); !errors.Is(err, ErrRejected) {
				t.Fatalf("Dial = %v; want ErrRejected", err)
			}
		},
		"unknown frame to the dialer": func(t *testing.T, fd int) {
			if err := answerHello(t, frameDoorbell, fd); err != errProtocol {
				t.Fatalf("Dial = %v; want errProtocol", err)
			}
		},
		"malformed Welcome to the dialer": func(t *testing.T, fd int) {
			if err := answerHello(t, frameWelcome, fd); err != errProtocol {
				t.Fatalf("Dial = %v; want errProtocol", err)
			}
		},
		"frame on an open connection": func(t *testing.T, fd int) {
			lc, _ := pair(t, ListenConfig{}, Dialer{})
			if err := lc.link.Send(frameDoorbell, []byte{dirListenerToDialer}, []int{fd}); err != nil {
				t.Fatal(err)
			}
		},
	} {
		t.Run(name, func(t *testing.T) {
			r, w, err := os.Pipe()
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()
			deliver(t, int(w.Fd()))
			w.Close()
			r.SetReadDeadline(time.Now().Add(5 * time.Second))
			if _, err := r.Read(make([]byte, 1)); err != io.EOF {
				t.Fatalf("pipe Read = %v; want io.EOF, every copy of its write end closed", err)
			}
		})
	}
}
