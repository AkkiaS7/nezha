//go:build linux || darwin || freebsd

package singleton

import "golang.org/x/sys/unix"

// diskFree returns the space available to the dashboard on the file system
// holding dir, or 0 when it cannot be determined.
func diskFree(dir string) int64 {
	var st unix.Statfs_t
	if err := unix.Statfs(dir, &st); err != nil {
		return 0
	}
	return int64(st.Bavail) * int64(st.Bsize)
}
