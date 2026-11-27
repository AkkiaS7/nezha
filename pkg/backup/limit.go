package backup

import (
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
)

const (
	// EnvMaxSizeMB overrides the size limit of backups in MiB. It is only
	// read from the environment, never from settings the admin panel can
	// change: the limit guards restores against oversized or crafted files,
	// so loosening it requires access to the host.
	EnvMaxSizeMB = "NZ_BACKUP_MAX_SIZE_MB"
	// MinMaxSizeMB and MaxMaxSizeMB bound EnvMaxSizeMB.
	MinMaxSizeMB = 64
	MaxMaxSizeMB = 64 << 10

	// payloadOverhead is what the decompressed payload may hold on top of
	// its entries: tar headers, padding and the end-of-archive blocks.
	payloadOverhead int64 = 8 << 20
	// diskReserve is the free space a backup or restore leaves untouched so
	// the live database can still grow while it runs.
	diskReserve int64 = 256 << 20
)

var (
	ErrInvalidMaxSize    = fmt.Errorf("%s must be a whole number of MiB between %d and %d", EnvMaxSizeMB, MinMaxSizeMB, MaxMaxSizeMB)
	ErrInsufficientSpace = errors.New("not enough free disk space in the data directory")
)

// ParseMaxSizeMB parses a value of EnvMaxSizeMB into bytes. An empty value
// selects DefaultMaxBackupBytes.
func ParseMaxSizeMB(v string) (int64, error) {
	v = strings.TrimSpace(v)
	if v == "" {
		return DefaultMaxBackupBytes, nil
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil || n < MinMaxSizeMB || n > MaxMaxSizeMB {
		return 0, ErrInvalidMaxSize
	}
	return n << 20, nil
}

// MaxBytesFromEnv returns the size limit configured by EnvMaxSizeMB. An
// invalid value is logged and the default is used, so a typo never disables
// backups.
func MaxBytesFromEnv() int64 {
	v := os.Getenv(EnvMaxSizeMB)
	n, err := ParseMaxSizeMB(v)
	if err != nil {
		log.Printf("NEZHA>> backup: %v; using the default of %d MiB", err, DefaultMaxBackupBytes>>20)
		return DefaultMaxBackupBytes
	}
	if strings.TrimSpace(v) != "" {
		// #nosec G706 -- n is an integer validated by ParseMaxSizeMB, not raw input
		log.Printf("NEZHA>> backup: size limit set to %d MiB by %s", n>>20, EnvMaxSizeMB)
	}
	return n
}

// SpaceError reports a backup or restore step that would leave less than
// diskReserve free in the data directory.
type SpaceError struct {
	Need, Free int64
}

func (e *SpaceError) Error() string {
	return fmt.Sprintf("%s: %d MiB needed, %d MiB free", ErrInsufficientSpace.Error(), e.Need>>20, e.Free>>20)
}

// Is makes errors.Is(err, ErrInsufficientSpace) hold.
func (e *SpaceError) Is(target error) bool { return target == ErrInsufficientSpace }

// EnsureSpace checks that n more bytes can be written to the data directory
// while leaving diskReserve free. When the free space cannot be determined
// the step is allowed: the check protects the live database, it must not
// make backups unusable on file systems that do not report it.
func (m *Manager) EnsureSpace(n int64) error {
	if n < 0 {
		n = 0
	}
	free, err := m.opts.DiskFree(m.opts.DataDir)
	if err != nil {
		log.Printf("NEZHA>> backup: cannot read the free disk space: %v", err)
		return nil
	}
	need := n + diskReserve
	if free < need {
		return &SpaceError{Need: need, Free: free}
	}
	return nil
}

// spaceCheckInterval is how much a guarded writer writes between two
// checks of the free space.
const spaceCheckInterval int64 = 8 << 20

// guardedWriter checks the free disk space before every spaceCheckInterval
// bytes it writes (or before the rest of the stream, when its size is known
// and smaller), so a stream of unknown length stops before it eats into
// diskReserve. Streams written at the same time can each pass one check
// before the other writes, so they may overshoot by at most one interval
// each; uploads are therefore received one at a time (AcquireUpload).
type guardedWriter struct {
	w        io.Writer
	check    func(int64) error
	expected int64 // total size of the stream, 0 when unknown
	written  int64
	budget   int64
}

// guardSpace wraps w with check; a nil check returns w unchanged. expected
// is the size of the stream when known, otherwise 0.
func guardSpace(w io.Writer, check func(int64) error, expected int64) io.Writer {
	if check == nil {
		return w
	}
	return &guardedWriter{w: w, check: check, expected: expected}
}

func (g *guardedWriter) Write(p []byte) (int, error) {
	if n := int64(len(p)); n > g.budget {
		next := spaceCheckInterval
		if g.expected > 0 {
			next = min(next, g.expected-g.written)
		}
		next = max(next, n)
		if err := g.check(next); err != nil {
			return 0, err
		}
		g.budget = next
	}
	n, err := g.w.Write(p)
	g.budget -= int64(n)
	g.written += int64(n)
	return n, err
}

// AcquireUpload reserves the only slot for receiving an uploaded backup.
// The caller holds it until the upload has been checked (Prepare returned),
// so at most one uploaded file is staged at a time. It fails with ErrBusy
// while another upload holds it.
func (m *Manager) AcquireUpload() (release func(), err error) {
	if !m.uploadMu.TryLock() {
		return nil, ErrBusy
	}
	return m.uploadMu.Unlock, nil
}

// SaveUpload writes an uploaded backup below the data directory and
// returns its path. It enforces the size limit and the free space while
// writing, so it does not rely on the declared length of the request, and
// removes the file when it fails. sizeHint is an upper bound of the size
// when the request declared its length, otherwise 0.
func (m *Manager) SaveUpload(r io.Reader, sizeHint int64) (string, error) {
	f, err := m.UploadTempFile()
	if err != nil {
		return "", err
	}
	n, err := io.Copy(guardSpace(f, m.EnsureSpace, max(sizeHint, 0)), io.LimitReader(r, m.opts.MaxBackupBytes+1))
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err == nil && n > m.opts.MaxBackupBytes {
		err = tooLargeError(n, m.opts.MaxBackupBytes)
	}
	if err != nil {
		_ = os.Remove(f.Name())
		return "", err
	}
	return f.Name(), nil
}

// capReader fails once more than n bytes are read.
type capReader struct {
	r        io.Reader
	n        int64
	exceeded bool
}

func (c *capReader) Read(p []byte) (int, error) {
	if c.n <= 0 {
		c.exceeded = true
		return 0, ErrTooLarge
	}
	if int64(len(p)) > c.n {
		p = p[:c.n]
	}
	n, err := c.r.Read(p)
	c.n -= int64(n)
	return n, err
}
