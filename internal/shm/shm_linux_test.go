package shm

import (
	"os"
	"strconv"
	"strings"
	"testing"

	"golang.org/x/sys/unix"
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

func TestExportedSegmentRefusesNewSeals(t *testing.T) {
	if _, err := unix.FcntlInt(uintptr(exportedFD(t, MinSize)), unix.F_ADD_SEALS, unix.F_SEAL_FUTURE_WRITE); err == nil {
		t.Fatal("F_ADD_SEALS succeeded on an exported segment's fd")
	}
}

func TestAttachRefusesResizableMemfd(t *testing.T) {
	for _, tc := range []struct {
		name  string
		flags int
		seals int
	}{
		{"unsealed", unix.MFD_CLOEXEC, 0},
		{"can shrink", unix.MFD_CLOEXEC | unix.MFD_ALLOW_SEALING, unix.F_SEAL_GROW},
		{"can grow", unix.MFD_CLOEXEC | unix.MFD_ALLOW_SEALING, unix.F_SEAL_SHRINK},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fd, err := unix.MemfdCreate("peer", tc.flags)
			if err != nil {
				t.Fatal(err)
			}
			if err := unix.Ftruncate(fd, int64(mapLen(MinSize))); err != nil {
				unix.Close(fd)
				t.Fatal(err)
			}
			if tc.seals != 0 {
				if _, err := unix.FcntlInt(uintptr(fd), unix.F_ADD_SEALS, tc.seals); err != nil {
					unix.Close(fd)
					t.Fatal(err)
				}
			}
			assertAttachRefuses(t, fd, MinSize)
		})
	}
}
