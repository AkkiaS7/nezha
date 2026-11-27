//go:build !windows

package backup

import (
	"math"

	"golang.org/x/sys/unix"
)

// diskFree returns the bytes available to the process on the file system of path.
func diskFree(path string) (int64, error) {
	var st unix.Statfs_t
	if err := unix.Statfs(path, &st); err != nil {
		return 0, err
	}
	free := uint64(st.Bavail) * uint64(st.Bsize) // #nosec G115 -- block counts and sizes are never negative
	if free > math.MaxInt64 {
		return math.MaxInt64, nil
	}
	return int64(free), nil
}
