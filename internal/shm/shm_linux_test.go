package shm

import (
	"os"
	"strconv"
	"strings"
	"testing"
)

func listDevShm(t *testing.T) map[string]bool {
	t.Helper()
	entries, err := os.ReadDir("/dev/shm")
	if err != nil {
		t.Skipf("/dev/shm unavailable: %v", err)
	}
	names := make(map[string]bool, len(entries))
	for _, e := range entries {
		names[e.Name()] = true
	}
	return names
}

func TestLinuxCreateUsesMemfdWithoutDevShmResidue(t *testing.T) {
	before := listDevShm(t)
	seg, err := Create(MinSize)
	if err != nil {
		t.Fatal(err)
	}
	defer seg.Close()

	link, err := os.Readlink("/proc/self/fd/" + strconv.Itoa(seg.sys.fd))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(link, "/memfd:") {
		t.Fatalf("segment fd links to %q, want /memfd:...", link)
	}
	for name := range listDevShm(t) {
		if !before[name] {
			t.Fatalf("new /dev/shm entry %q after Create", name)
		}
	}
}
