//go:build !linux && !darwin && !freebsd && !windows

package singleton

// diskFree is unknown on this platform; callers treat 0 as "do not check".
func diskFree(string) int64 { return 0 }
