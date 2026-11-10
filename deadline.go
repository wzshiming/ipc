package ipc

import (
	"sync"
	"sync/atomic"
	"time"
)

// deadline is the deadline of one direction of a connection: once it
// passes, expired is set and the channel done returns is closed. A later
// deadline replaces a closed channel with an open one.
type deadline struct {
	expired atomic.Bool
	mu      sync.Mutex
	timer   *time.Timer
	ch      chan struct{}
}

func (d *deadline) set(t time.Time) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.timer != nil && !d.timer.Stop() {
		<-d.ch // the timer fired: wait until it has closed ch
	}
	d.timer = nil
	if d.ch == nil || isClosed(d.ch) {
		if d.ch != nil && !t.IsZero() && !time.Now().Before(t) {
			return
		}
		d.ch = make(chan struct{})
		d.expired.Store(false)
	}
	if t.IsZero() {
		return
	}
	ch := d.ch
	expire := func() {
		d.expired.Store(true)
		close(ch)
	}
	if dur := time.Until(t); dur > 0 {
		d.timer = time.AfterFunc(dur, expire)
	} else {
		expire()
	}
}

// done returns a channel closed once the deadline passes.
func (d *deadline) done() <-chan struct{} {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.ch == nil {
		d.ch = make(chan struct{})
	}
	return d.ch
}

func isClosed(ch chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}
