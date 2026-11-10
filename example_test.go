package ipc_test

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"

	"github.com/wzshiming/ipc"
)

// The listener and the dialer are normally two processes.
func Example() {
	dir, _ := os.MkdirTemp("", "ipc") // short: sun_path is ≤ 104 bytes on macOS
	defer os.RemoveAll(dir)
	path := filepath.Join(dir, "s")
	l, err := (&ipc.ListenConfig{Lanes: 2}).Listen(context.Background(), path)
	if err != nil {
		log.Fatal(err)
	}
	defer l.Close()
	go func() {
		c, err := (&ipc.Dialer{Lanes: 2}).Dial(path)
		if err != nil {
			log.Fatal(err)
		}
		defer c.Close()
		p, err := c.Lane(0).Reserve(len("hello"))
		if err != nil {
			log.Fatal(err)
		}
		copy(p, "hello") // fill the record in shared memory
		c.Lane(0).Commit()
		s := ipc.NewStream(c.Lane(1)) // lane 1 as a net.Conn byte stream
		io.WriteString(s, "hello, stream")
		s.Close() // the peer's Read sees io.EOF
	}()
	c, err := l.Accept()
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()
	rec, _ := c.Lane(0).Next() // rec aliases the ring until Release
	fmt.Printf("%d lanes, lane 0: %q\n", c.Lanes(), rec)
	c.Lane(0).Release()
	b, err := io.ReadAll(ipc.NewStream(c.Lane(1)))
	fmt.Printf("lane 1: %q, %v\n", b, err)
	// Output:
	// 2 lanes, lane 0: "hello"
	// lane 1: "hello, stream", <nil>
}
