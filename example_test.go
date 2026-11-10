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
		c, err := ipc.Dial(path) // a net.Conn
		if err != nil {
			log.Fatal(err)
		}
		defer c.Close()
		io.WriteString(c, "hello")
		reply := make([]byte, len("hello, stream"))
		io.ReadFull(c, reply)
		_, err = c.Read(reply) // the listener's Close ends the stream
		fmt.Printf("dialer read %q, then %v\n", reply, err)
	}()
	c, err := l.Accept() // a net.Conn
	if err != nil {
		log.Fatal(err)
	}
	b := make([]byte, len("hello"))
	io.ReadFull(c, b)
	fmt.Printf("listener got %q\n", b)
	io.WriteString(c, "hello, stream")
	c.Close()
	<-done
	// Output:
	// listener got "hello"
	// dialer read "hello, stream", then EOF
}
