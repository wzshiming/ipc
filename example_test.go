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

// connect listens on a fresh socket and returns both ends of one connection.
// In real use the listener and the dialer are two processes.
func connect(lc *ipc.ListenConfig, d *ipc.Dialer) (server, client *ipc.Conn, cleanup func()) {
	dir, err := os.MkdirTemp("", "ipc") // keep the path short: sun_path is ≤ 104 bytes on macOS
	if err != nil {
		log.Fatal(err)
	}
	path := filepath.Join(dir, "s")

	l, err := lc.Listen(context.Background(), path)
	if err != nil {
		log.Fatal(err)
	}
	accepted := make(chan *ipc.Conn, 1)
	go func() {
		c, err := l.Accept()
		if err != nil {
			log.Fatal(err)
		}
		accepted <- c
	}()

	client, err = d.Dial(path)
	if err != nil {
		log.Fatal(err)
	}
	server = <-accepted
	return server, client, func() {
		client.Close()
		server.Close()
		l.Close()
		os.RemoveAll(dir)
	}
}

// Records are written and read in place in shared memory.
func Example() {
	server, client, cleanup := connect(
		&ipc.ListenConfig{Lanes: 2, RingSize: 64 << 10},
		&ipc.Dialer{Lanes: 2, RingSize: 64 << 10},
	)
	defer cleanup()
	fmt.Println("lanes:", client.Lanes(), "max record:", client.MaxRecord())

	tx := client.Lane(1)
	p, err := tx.Reserve(len("hello"))
	if err != nil {
		log.Fatal(err)
	}
	copy(p, "hello") // fill the record where the peer will read it
	if err := tx.Commit(); err != nil {
		log.Fatal(err)
	}

	rx := server.Lane(1)
	rec, err := rx.Next()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("lane %d got %q\n", rx.Index(), rec) // rec aliases the ring until Release
	if err := rx.Release(); err != nil {
		log.Fatal(err)
	}
	// Output:
	// lanes: 2 max record: 32760
	// lane 1 got "hello"
}

// NewStream turns a lane into a net.Conn byte stream.
func ExampleNewStream() {
	server, client, cleanup := connect(nil, nil) // nil configs use the defaults
	defer cleanup()

	w := ipc.NewStream(client.Lane(0))
	if _, err := io.WriteString(w, "hello, stream"); err != nil {
		log.Fatal(err)
	}
	w.Close() // the peer's Read sees io.EOF

	r := ipc.NewStream(server.Lane(0))
	defer r.Close()
	b, err := io.ReadAll(r)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(string(b))
	// Output:
	// hello, stream
}
