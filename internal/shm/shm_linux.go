package shm

import "golang.org/x/sys/unix"

func createFD() (int, string, error) {
	fd, err := unix.MemfdCreate("ipc", unix.MFD_CLOEXEC)
	return fd, "", err
}
