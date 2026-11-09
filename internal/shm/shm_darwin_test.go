package shm

import (
	"errors"
	"testing"

	"golang.org/x/sys/unix"
)

func TestDarwinCreateLeavesNoShmName(t *testing.T) {
	seg, err := Create(MinSize)
	if err != nil {
		t.Fatal(err)
	}
	defer seg.Close()
	if seg.sys.name == "" {
		t.Fatal("segment did not record its shm name")
	}
	fd, err := shmOpen(seg.sys.name, unix.O_RDONLY, 0)
	if err == nil {
		unix.Close(fd)
		t.Fatalf("shm_open(%q) succeeded after Create, want ENOENT", seg.sys.name)
	}
	if !errors.Is(err, unix.ENOENT) {
		t.Fatalf("shm_open(%q) = %v, want ENOENT", seg.sys.name, err)
	}
}

func TestDarwinShmOpenPassesModeAndExcl(t *testing.T) {
	name, err := randomShmName()
	if err != nil {
		t.Fatal(err)
	}
	fd, err := shmOpen(name, unix.O_RDWR|unix.O_CREAT|unix.O_EXCL, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer unix.Close(fd)
	defer shmUnlink(name)
	assertMode(t, fd, 0o600)

	// A second mode rules out the variadic argument matching by accident.
	other, err := randomShmName()
	if err != nil {
		t.Fatal(err)
	}
	ofd, err := shmOpen(other, unix.O_RDWR|unix.O_CREAT|unix.O_EXCL, 0o640)
	if err != nil {
		t.Fatal(err)
	}
	defer unix.Close(ofd)
	defer shmUnlink(other)
	assertMode(t, ofd, 0o640)

	fd2, err := shmOpen(name, unix.O_RDWR|unix.O_CREAT|unix.O_EXCL, 0o600)
	if err == nil {
		unix.Close(fd2)
		t.Fatal("second O_CREAT|O_EXCL shm_open succeeded, want EEXIST")
	}
	if !errors.Is(err, unix.EEXIST) {
		t.Fatalf("second shm_open = %v, want EEXIST", err)
	}
}

func assertMode(t *testing.T, fd int, want uint16) {
	t.Helper()
	var st unix.Stat_t
	if err := unix.Fstat(fd, &st); err != nil {
		t.Fatal(err)
	}
	if perm := st.Mode & 0o777; perm != want {
		t.Fatalf("mode = %#o, want %#o", perm, want)
	}
}

func TestDarwinShmNameFitsPSHMNAMLEN(t *testing.T) {
	name, err := randomShmName()
	if err != nil {
		t.Fatal(err)
	}
	if len(name) > 31 || name[0] != '/' {
		t.Fatalf("name %q: want leading '/' and at most 31 chars", name)
	}
}
