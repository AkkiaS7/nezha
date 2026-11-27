//go:build windows

package singleton

import "golang.org/x/sys/windows"

// diskFree returns the space available to the dashboard on the volume
// holding dir, or 0 when it cannot be determined.
func diskFree(dir string) int64 {
	p, err := windows.UTF16PtrFromString(dir)
	if err != nil {
		return 0
	}
	var avail, total, free uint64
	if err := windows.GetDiskFreeSpaceEx(p, &avail, &total, &free); err != nil {
		return 0
	}
	return int64(avail)
}
