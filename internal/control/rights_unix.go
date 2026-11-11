//go:build darwin || linux

package control

import (
	"errors"
	"io"
	"net"
	"syscall"

	"golang.org/x/sys/unix"
)

const fdPassing = true

var oobSize = unix.CmsgSpace(MaxFDs * 4)

var errTruncatedRights = errors.New("control: fd attachment truncated")

// writeFirst sends the frame's first byte alone so its rights cannot ride on payload bytes.
func writeFirst(c *net.UnixConn, b []byte, fds []int) error {
	_, _, err := c.WriteMsgUnix(b, unix.UnixRights(fds...), nil)
	return err
}

func readFirst(c *net.UnixConn, b, oob []byte) ([]int, error) {
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
	if err == nil && n == 0 {
		err = io.EOF
	}
	if err != nil {
		CloseFDs(fds)
		return nil, err
	}
	return fds, nil
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
