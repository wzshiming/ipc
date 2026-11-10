package ipc_test

import (
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
	l, err := ipc.Listen(path)
	if err != nil {
		log.Fatal(err)
	}
	defer l.Close()
	done := make(chan struct{})
	go func() {
		defer close(done)
		c, err := ipc.Dial(path)
		if err != nil {
			log.Fatal(err)
		}
		defer c.Close()
		p, err := c.Reserve(len("hello")) // the record API: p is shared memory
		if err != nil {
			log.Fatal(err)
		}
		copy(p, "hello")
		c.Commit()
		b, err := io.ReadAll(c) // the byte stream the listener sends, until its Close
		fmt.Printf("dialer read %q, %v\n", b, err)
	}()
	nc, err := l.Accept() // a net.Conn
	if err != nil {
		log.Fatal(err)
	}
	c := nc.(*ipc.Conn) // *Conn adds the record API
	rec, _ := c.Next()  // rec aliases the ring until Release
	fmt.Printf("listener got %q\n", rec)
	c.Release()
	io.WriteString(c, "hello, stream")
	c.Close() // the dialer's Read sees io.EOF
	<-done
	// Output:
	// listener got "hello"
	// dialer read "hello, stream", <nil>
}
