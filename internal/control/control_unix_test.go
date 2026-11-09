//go:build darwin || linux

package control

import (
	"os"
	"testing"
)

func TestListenSocketFileIsOwnerOnly(t *testing.T) {
	path := sockPath(t)
	listen(t, path)
	fi, err := os.Lstat(path)
	if err != nil {
		t.Fatal(err)
	}
	if perm := fi.Mode().Perm(); perm != 0o600 {
		t.Fatalf("socket file mode %v, want 0600", perm)
	}
}
