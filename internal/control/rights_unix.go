//go:build darwin || linux

package control

import (
	"errors"
	"net"
	"syscall"

	"golang.org/x/sys/unix"
)

const fdPassing = true

// maxRights is the most descriptors one message can carry: 253 on linux
// (SCM_MAX_FD), 254 on darwin (2 KiB of 8-byte file pointers).
const maxRights = 254

// oobSize fits the largest control message, so that none is ever cut.
var oobSize = unix.CmsgSpace(maxRights * 4)

var errTruncatedRights = errors.New("control: fd attachment truncated")

// writeFirst sends the frame's first byte alone so its rights cannot ride on payload bytes.
func writeFirst(c *net.UnixConn, b []byte, fds []int) error {
	_, _, err := c.WriteMsgUnix(b, unix.UnixRights(fds...), nil)
	return err
}

// readMsg reads into b and returns the descriptors that came with the bytes
// read. Every read asks for them: darwin installs them even when not asked.
func readMsg(c *net.UnixConn, b, oob []byte) (int, []int, error) {
	n, oobn, flags, _, err := c.ReadMsgUnix(b, oob)
	var fds []int
	if oobn > 0 {
		var perr error
		fds, perr = parseRights(oob[:oobn])
		if err == nil {
			err = perr
		}
	}
	if err == nil && flags&unix.MSG_CTRUNC != 0 {
		err = errTruncatedRights
	}
	return n, fds, err
}

func parseRights(oob []byte) ([]int, error) {
	msgs, err := unix.ParseSocketControlMessage(oob)
	if err != nil {
		return nil, err
	}
	var fds []int
	for i := range msgs {
		if msgs[i].Header.Level != unix.SOL_SOCKET || msgs[i].Header.Type != unix.SCM_RIGHTS {
			continue
		}
		got, err := unix.ParseUnixRights(&msgs[i])
		if err != nil {
			CloseFDs(fds)
			return nil, err
		}
		fds = append(fds, got...)
	}
	return fds, nil
}

// CloseFDs closes descriptors that Recv returned.
func CloseFDs(fds []int) {
	for _, fd := range fds {
		unix.Close(fd)
	}
}

func isConnRefused(err error) bool { return errors.Is(err, syscall.ECONNREFUSED) }

func unsupportedErr(err error) error { return err }
