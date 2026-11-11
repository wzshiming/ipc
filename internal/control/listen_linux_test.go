package control

import (
	"errors"
	"os"
	"strconv"
	"syscall"
	"testing"
)

// abstractName returns an abstract socket name no other test process uses.
func abstractName(t *testing.T) string {
	return "@ipc-control-" + strconv.Itoa(os.Getpid()) + "-" + t.Name()
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
