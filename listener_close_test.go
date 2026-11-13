package ipc

import "testing"

func TestListenerCloseConcurrentCallsReturnOneResult(t *testing.T) {
	l, _ := listen(t, ListenConfig{})
	const callers = 16
	start := make(chan struct{})
	errc := make(chan error, callers)
	for range callers {
		go func() {
			<-start
			errc <- l.Close()
		}()
	}
	close(start)
	first := <-errc
	for range callers - 1 {
		if err := <-errc; err != first {
			t.Fatalf("concurrent Close calls returned two different values, %v and %v", first, err)
		}
	}
}

func TestListenerCloseRepeatsTheFirstCallsError(t *testing.T) {
	l, _ := listen(t, ListenConfig{})
	if err := l.cl.Close(); err != nil {
		t.Fatal(err)
	}
	first := l.Close()
	if first == nil {
		t.Fatal("Close over an already closed control listener returned nil")
	}
	if err := l.Close(); err != first {
		t.Fatalf("second Close returned %v, a different value from the first call's %v", err, first)
	}
}
