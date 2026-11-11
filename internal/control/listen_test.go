//go:build darwin || linux || windows

package control

import (
	"context"
	"fmt"
	"testing"
	"time"
)

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
