package shm

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"unsafe"

	"golang.org/x/sys/unix"
)

// shm_open/shm_unlink are absent from x/sys/unix; call libSystem through
// trampolines the same way x/sys's zsyscall_darwin does.

//go:cgo_import_dynamic libc_shm_open shm_open "/usr/lib/libSystem.B.dylib"
//go:cgo_import_dynamic libc_shm_unlink shm_unlink "/usr/lib/libSystem.B.dylib"

var (
	libc_shm_open_trampoline_addr   uintptr
	libc_shm_unlink_trampoline_addr uintptr
)

// syscall.syscall places a3 on the stack too on arm64, as the variadic mode needs.
//
//go:linkname syscall_syscall syscall.syscall
func syscall_syscall(fn, a1, a2, a3 uintptr) (r1, r2 uintptr, err unix.Errno)

func shmOpen(name string, flag int, mode uint32) (int, error) {
	p, err := unix.BytePtrFromString(name)
	if err != nil {
		return -1, err
	}
	r, _, e := syscall_syscall(libc_shm_open_trampoline_addr, uintptr(unsafe.Pointer(p)), uintptr(flag), uintptr(mode))
	if e != 0 {
		return -1, e
	}
	return int(r), nil
}

func shmUnlink(name string) error {
	p, err := unix.BytePtrFromString(name)
	if err != nil {
		return err
	}
	if _, _, e := syscall_syscall(libc_shm_unlink_trampoline_addr, uintptr(unsafe.Pointer(p)), 0, 0); e != 0 {
		return e
	}
	return nil
}

// randomShmName returns "/ipc-" + 24 hex chars: 29 chars, within PSHMNAMLEN (31).
func randomShmName() (string, error) {
	var b [12]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return "/ipc-" + hex.EncodeToString(b[:]), nil
}

func createFD() (int, string, error) {
	for range 8 {
		name, err := randomShmName()
		if err != nil {
			return -1, "", err
		}
		fd, err := shmOpen(name, unix.O_RDWR|unix.O_CREAT|unix.O_EXCL, 0o600)
		if errors.Is(err, unix.EEXIST) {
			continue
		}
		if err != nil {
			return -1, "", err
		}
		if err := shmUnlink(name); err != nil {
			unix.Close(fd)
			return -1, "", err
		}
		unix.CloseOnExec(fd)
		return fd, name, nil
	}
	return -1, "", unix.EEXIST
}

// sealSize has nothing to do: the kernel refuses to resize a POSIX shm
// object once ftruncate has sized it.
func sealSize(int) error { return nil }

// checkSizeSealed fails unless fd is a POSIX shm object, whose size is final
// once set; fstat reports no file type for that kind of descriptor alone.
func checkSizeSealed(fd int) error {
	var st unix.Stat_t
	if err := unix.Fstat(fd, &st); err != nil {
		return err
	}
	if st.Mode&unix.S_IFMT != 0 {
		return errors.New("shm: segment is not a POSIX shared-memory object")
	}
	return nil
}
