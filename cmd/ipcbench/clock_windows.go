//go:build windows

package main

import (
	"time"
	"unsafe"

	"golang.org/x/sys/windows"
)

// Go's clock on Windows ticks in 0.5–15.6 ms steps; QPC resolves ~100 ns.
var (
	kernel32 = windows.NewLazySystemDLL("kernel32.dll")
	qpc      = kernel32.NewProc("QueryPerformanceCounter")
	qpcFreq  = func() int64 {
		var f int64
		if r, _, err := kernel32.NewProc("QueryPerformanceFrequency").Call(uintptr(unsafe.Pointer(&f))); r == 0 {
			panic("QueryPerformanceFrequency: " + err.Error())
		}
		return f
	}()
)

// now returns QueryPerformanceCounter ticks as a duration.
func now() time.Duration {
	var t int64
	qpc.Call(uintptr(unsafe.Pointer(&t)))
	// Split so ticks*1e9 cannot overflow on long uptimes.
	return time.Duration(t/qpcFreq*1e9 + t%qpcFreq*1e9/qpcFreq)
}
