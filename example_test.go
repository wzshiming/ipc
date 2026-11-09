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
func connect(ctx context.Context, cfg ipc.Config) (server, client *ipc.Conn, cleanup func()) {
	dir, err := os.MkdirTemp("", "ipc") // keep the path short: sun_path is ≤ 104 bytes on macOS
	if err != nil {
		log.Fatal(err)
	}
	path := filepath.Join(dir, "s")

	l, err := ipc.Listen(path, cfg)
	if err != nil {
		log.Fatal(err)
	}
	accepted := make(chan *ipc.Conn, 1)
	go func() {
		c, err := l.Accept(ctx)
		if err != nil {
			log.Fatal(err)
		}
		accepted <- c
	}()

	client, err = ipc.Dial(ctx, path, cfg)
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
	ctx := context.Background()
	server, client, cleanup := connect(ctx, ipc.Config{Lanes: 2, RingSize: 64 << 10})
	defer cleanup()
	fmt.Println("lanes:", client.Lanes(), "max record:", client.MaxRecord())

	tx := client.Lane(1)
	p, err := tx.Reserve(ctx, len("hello"))
	if err != nil {
		log.Fatal(err)
	}
	copy(p, "hello") // fill the record where the peer will read it
	if err := tx.Commit(); err != nil {
		log.Fatal(err)
	}

	rx := server.Lane(1)
	rec, err := rx.Next(ctx)
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
	ctx := context.Background()
	server, client, cleanup := connect(ctx, ipc.Config{})
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
