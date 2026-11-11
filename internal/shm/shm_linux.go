package shm

import (
	"errors"

	"golang.org/x/sys/unix"
)

// sizeSeals keep a memfd from shrinking under a peer's mapping, or growing.
const sizeSeals = unix.F_SEAL_SHRINK | unix.F_SEAL_GROW

func createFD() (int, string, error) {
	fd, err := unix.MemfdCreate("ipc", unix.MFD_CLOEXEC|unix.MFD_ALLOW_SEALING)
	return fd, "", err
}

// sealSize makes the memfd's current size final, and its seals with it.
func sealSize(fd int) error {
	_, err := unix.FcntlInt(uintptr(fd), unix.F_ADD_SEALS, sizeSeals|unix.F_SEAL_SEAL)
	return err
}

// checkSizeSealed fails unless fd is a memfd sealed against shrinking and growing.
func checkSizeSealed(fd int) error {
	seals, err := unix.FcntlInt(uintptr(fd), unix.F_GET_SEALS, 0)
	if err != nil || seals&sizeSeals != sizeSeals {
		return errors.New("shm: segment is not a memfd sealed against resizing")
	}
	return nil
}
