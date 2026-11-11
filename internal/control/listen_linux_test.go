package control

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"syscall"
	"testing"
	"time"
)

// abstractName returns an abstract socket name no other test process uses.
func abstractName(t *testing.T) string {
	return "@ipc-control-" + strconv.Itoa(os.Getpid()) + "-" + t.Name()
}

// roundTrip dials path and waits until l accepts the link that carries the
// frame it sent, closing links others dialed.
func roundTrip(l *Listener, path string) error {
	d, err := Dial(context.Background(), path)
	if err != nil {
		return err
	}
	defer d.Close()
	if err := d.Send(7, []byte("ping"), nil); err != nil {
		return err
	}
	deadline := time.Now().Add(5 * time.Second)
	l.SetDeadline(deadline)
	defer l.SetDeadline(time.Time{})
	for {
		a, err := l.Accept()
		if err != nil {
			return fmt.Errorf("Accept: %w", err)
		}
		a.SetReadDeadline(deadline)
		typ, p, _, err := a.Recv()
		a.Close()
		if err == nil && typ == 7 && string(p) == "ping" {
			return nil
		}
	}
}

// acceptOne fails the test unless l accepts a link dialed to path.
func acceptOne(t *testing.T, l *Listener, path string) {
	t.Helper()
	if err := roundTrip(l, path); err != nil {
		t.Fatal(err)
	}
}

func TestListenOnAnAbstractNameServesAndLeavesNoFile(t *testing.T) {
	name := abstractName(t)
	noFiles := func(when string) {
		t.Helper()
		for _, p := range []string{name, name + ".lock"} {
			if _, err := os.Lstat(p); !os.IsNotExist(err) {
				t.Fatalf("%s: file %s exists (%v)", when, p, err)
			}
		}
	}
	l := listen(t, name)
	acceptOne(t, l, name)
	noFiles("while listening")
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	noFiles("after Close")
}

func TestListenOnALiveAbstractNameIsAddrInUse(t *testing.T) {
	name := abstractName(t)
	listen(t, name)
	l, err := Listen(t.Context(), name)
	if err == nil {
		l.Close()
		t.Fatal("second Listen on a live abstract name succeeded")
	}
	if !errors.Is(err, syscall.EADDRINUSE) {
		t.Fatalf("got %v, want EADDRINUSE", err)
	}
}
