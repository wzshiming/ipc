//go:build windows

package main

import (
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

// nowNanos returns QueryPerformanceCounter ticks as nanoseconds.
func nowNanos() int64 {
	var t int64
	qpc.Call(uintptr(unsafe.Pointer(&t)))
	// Split so ticks*1e9 cannot overflow on long uptimes.
	return t/qpcFreq*1e9 + t%qpcFreq*1e9/qpcFreq
}
