package backup

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseMaxSizeMB(t *testing.T) {
	for v, want := range map[string]int64{
		"":        DefaultMaxBackupBytes,
		"  ":      DefaultMaxBackupBytes,
		"64":      64 << 20,
		" 2048 ":  2048 << 20,
		"65536":   65536 << 20,
		"1024":    1 << 30,
		"0000512": 512 << 20,
	} {
		got, err := ParseMaxSizeMB(v)
		require.NoError(t, err, v)
		assert.Equal(t, want, got, v)
	}
	for _, v := range []string{"63", "65537", "0", "-1", "1.5", "1G", "abc", "99999999999999999999"} {
		_, err := ParseMaxSizeMB(v)
		require.ErrorIs(t, err, ErrInvalidMaxSize, v)
	}
}

func TestMaxBytesFromEnv(t *testing.T) {
	t.Setenv(EnvMaxSizeMB, "")
	assert.Equal(t, DefaultMaxBackupBytes, MaxBytesFromEnv())
	t.Setenv(EnvMaxSizeMB, "4096")
	assert.Equal(t, int64(4096)<<20, MaxBytesFromEnv())
	// A typo never disables backups: the default applies.
	t.Setenv(EnvMaxSizeMB, "4 GiB")
	assert.Equal(t, DefaultMaxBackupBytes, MaxBytesFromEnv())
	t.Setenv(EnvMaxSizeMB, "1")
	assert.Equal(t, DefaultMaxBackupBytes, MaxBytesFromEnv())
}

// diskFree runs the real implementation of the platform (statfs on unix,
// GetDiskFreeSpaceEx on Windows).
func TestDiskFreeReportsSpace(t *testing.T) {
	free, err := DiskFree(t.TempDir())
	require.NoError(t, err)
	assert.Positive(t, free)
	_, err = DiskFree(filepath.Join(t.TempDir(), "missing", "dir"))
	assert.Error(t, err)
}

func TestEnsureSpace(t *testing.T) {
	free := int64(0)
	var freeErr error
	m := &Manager{opts: Options{DataDir: t.TempDir(), DiskFree: func(string) (int64, error) { return free, freeErr }}}

	free = diskReserve + 100
	require.NoError(t, m.EnsureSpace(100))
	err := m.EnsureSpace(101)
	require.ErrorIs(t, err, ErrInsufficientSpace)
	var se *SpaceError
	require.True(t, errors.As(err, &se))
	assert.Equal(t, diskReserve+101, se.Need)
	assert.Equal(t, diskReserve+100, se.Free)

	// A file system that does not report free space does not block backups.
	freeErr = errors.New("not supported")
	require.NoError(t, m.EnsureSpace(1<<40))
}

// paxBomb returns a gzip stream of a tar archive that repeats PAX extended
// headers. archive/tar reads them on its own, so no entry is ever returned
// and the entry sizes never count; only the cap on the decompressed stream
// stops it. It compresses about 1000:1.
func paxBomb(t *testing.T, decompressed int64) []byte {
	t.Helper()
	var one bytes.Buffer
	tw := tar.NewWriter(&one)
	require.NoError(t, tw.WriteHeader(&tar.Header{
		Name: FileDatabase, Typeflag: tar.TypeReg, Format: tar.FormatPAX,
		PAXRecords: map[string]string{"comment": strings.Repeat("a", 512<<10)},
	}))
	require.NoError(t, tw.Flush())
	// Drop the ustar header that follows the PAX header: only the PAX
	// header (its block and its records) is repeated.
	pax := one.Bytes()[:one.Len()-512]

	var out bytes.Buffer
	gz, err := gzip.NewWriterLevel(&out, gzip.BestCompression)
	require.NoError(t, err)
	for n := int64(0); n < decompressed; n += int64(len(pax)) {
		_, err := gz.Write(pax)
		require.NoError(t, err)
	}
	require.NoError(t, gz.Close())
	return out.Bytes()
}

func TestExtractStopsDecompressionBomb(t *testing.T) {
	const limit = 1 << 20
	bomb := paxBomb(t, 64<<20)
	require.Less(t, len(bomb), 1<<20, "the bomb itself is small")

	counted := &countingReader{r: bytes.NewReader(bomb)}
	start := time.Now()
	_, err := extractPayload(counted, t.TempDir(), limit, nil)
	require.ErrorIs(t, err, ErrTooLarge)
	assert.Less(t, time.Since(start), 10*time.Second)
	assert.Less(t, counted.n, int64(len(bomb)), "extraction stopped before the whole stream was read")
}

type countingReader struct {
	r io.Reader
	n int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	return n, err
}

// An entry larger than the limit is reported as too large, not as corrupt,
// so the administrator learns that the limit can be raised.
func TestExtractEntryOverLimitIsTooLarge(t *testing.T) {
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)
	data := bytes.Repeat([]byte("x"), 2<<20)
	require.NoError(t, tw.WriteHeader(&tar.Header{Name: FileDatabase, Typeflag: tar.TypeReg, Mode: 0600, Size: int64(len(data))}))
	_, err := tw.Write(data)
	require.NoError(t, err)
	require.NoError(t, tw.Close())
	require.NoError(t, gz.Close())

	dir := t.TempDir()
	_, err = extractPayload(bytes.NewReader(buf.Bytes()), dir, 1<<20, nil)
	require.ErrorIs(t, err, ErrTooLarge)
	assert.Contains(t, err.Error(), EnvMaxSizeMB)
	_, statErr := os.Stat(filepath.Join(dir, FileDatabase))
	assert.True(t, os.IsNotExist(statErr), "nothing is written for an entry over the limit")
}

// A backup written by upstream v2.8.0 (the first release with backups,
// format 1) is restored by this version. The file was produced by the
// v2.8.0 code with the test password of this package.
func TestRestoresV280Backup(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("testdata", "v2.8.0-manual.nzbak"))
	require.NoError(t, err)
	h, err := ReadHeader(bytes.NewReader(data))
	require.NoError(t, err)
	assert.Equal(t, "v2.8.0", h.NezhaVersion)

	e := newTestEnv(t)
	e.m = e.newManager("v2.9.0")
	e.configure()
	res, err := e.prepareUpload(data)
	require.NoError(t, err)
	require.True(t, res.CanApply, "%+v", res.Checks)
	assert.Equal(t, "v2.8.0", res.Manifest.NezhaVersion)
	assert.Contains(t, warningCodes(res), WarnOriginUnverified, "made by another dashboard")
	require.NoError(t, e.apply(res, testPassword))
}

// Backups and restores refuse to fill the data directory: each step checks
// the free space before writing and cleans up what it wrote.
func TestDiskSpaceChecks(t *testing.T) {
	e := newTestEnv(t)
	e.configure()
	b := e.backup(TriggerManual)
	require.Equal(t, StatusSuccess, b.Status, b.Error)

	var free int64
	e.diskFree = func(string) (int64, error) { return free, nil }
	e.advance(rateWindow)
	e.m = e.newManager("v1.2.3")

	// Backup: the snapshot and the file need twice the database size.
	free = diskReserve
	job, err := e.m.StartBackup(TriggerManual, admin)
	require.NoError(t, err)
	e.m.op.Lock()
	e.m.op.Unlock()
	list, err := e.m.List(context.Background())
	require.NoError(t, err)
	var failed BackupInfo
	for _, x := range list {
		if x.Name == job.Name {
			failed = x
		}
	}
	assert.Equal(t, StatusFailed, failed.Status)
	assert.Contains(t, failed.Error, ErrInsufficientSpace.Error())

	// Restore from storage: refused before the download.
	_, err = e.prepare(b, testPassword)
	require.ErrorIs(t, err, ErrInsufficientSpace)

	// Room for the download but not for the unpacked database.
	free = diskReserve + b.Size
	e.advance(rateWindow)
	res, err := e.prepare(b, testPassword)
	require.NoError(t, err)
	assert.False(t, res.CanApply)
	assert.Equal(t, CheckPayload, lastCheck(res).Code)
	assert.Contains(t, lastCheck(res).Message, ErrInsufficientSpace.Error())

	// Upload: the controller asks before streaming the body.
	require.ErrorIs(t, e.m.EnsureSpace(b.Size+1), ErrInsufficientSpace)

	entries, err := os.ReadDir(filepath.Join(e.dir, pendingDirName))
	require.NoError(t, err)
	assert.Empty(t, entries, "failed checks leave nothing staged")

	// Enough space: everything works again.
	free = 1 << 40
	e.advance(rateWindow)
	res, err = e.prepare(b, testPassword)
	require.NoError(t, err)
	assert.True(t, res.CanApply)
}

// fakeDisk reports the free space of a disk of a fixed size minus what has
// been written to the data directory, like a real file system would.
func fakeDisk(t *testing.T, dir string, free int64) func(string) (int64, error) {
	return func(string) (int64, error) {
		var used int64
		_ = filepath.WalkDir(dir, func(_ string, d os.DirEntry, err error) error {
			if err == nil && !d.IsDir() {
				if info, err := d.Info(); err == nil {
					used += info.Size()
				}
			}
			return nil
		})
		return free - used, nil
	}
}

func TestGuardedWriterChecksWhileWriting(t *testing.T) {
	var checks []int64
	allowed := int64(20 << 20)
	var buf bytes.Buffer
	w := guardSpace(&buf, func(n int64) error {
		checks = append(checks, n)
		if int64(buf.Len())+n > allowed {
			return &SpaceError{Need: n, Free: allowed - int64(buf.Len())}
		}
		return nil
	}, 0)
	chunk := bytes.Repeat([]byte("x"), 1<<20)
	var err error
	for range 64 {
		if _, err = w.Write(chunk); err != nil {
			break
		}
	}
	require.ErrorIs(t, err, ErrInsufficientSpace)
	assert.LessOrEqual(t, int64(buf.Len()), allowed, "it stops before the budget is exceeded")
	assert.Equal(t, int64(16<<20), int64(buf.Len()), "one check per 8 MiB")
	assert.Equal(t, []int64{8 << 20, 8 << 20, 8 << 20}, checks)

	// A single write larger than the interval is checked as a whole.
	checks = nil
	big := guardSpace(io.Discard, func(n int64) error { checks = append(checks, n); return nil }, 0)
	_, err = big.Write(make([]byte, 20<<20))
	require.NoError(t, err)
	assert.Equal(t, []int64{20 << 20}, checks)

	// A stream of known size only asks for what is left to write.
	checks = nil
	known := guardSpace(io.Discard, func(n int64) error { checks = append(checks, n); return nil }, 10<<20)
	for range 10 {
		_, err = known.Write(chunk)
		require.NoError(t, err)
	}
	assert.Equal(t, []int64{8 << 20, 2 << 20}, checks)

	assert.Equal(t, io.Writer(&buf), guardSpace(&buf, nil, 0))
}

// An upload of unknown length is held to the disk budget while it is
// written: it stops before cutting into the reserve and leaves no file.
func TestSaveUploadKeepsTheReserve(t *testing.T) {
	dir := t.TempDir()
	const room = 24 << 20
	m := &Manager{opts: Options{DataDir: dir, MaxBackupBytes: 1 << 30}}
	m.opts.DiskFree = fakeDisk(t, dir, diskReserve+room)

	first, err := m.SaveUpload(io.LimitReader(zeroReader{}, 20<<20), 0)
	require.NoError(t, err)
	_, err = m.SaveUpload(io.LimitReader(zeroReader{}, 20<<20), 0)
	require.ErrorIs(t, err, ErrInsufficientSpace)
	free, _ := m.opts.DiskFree(dir)
	assert.GreaterOrEqual(t, free, diskReserve, "the reserve stays free")
	entries, err := os.ReadDir(filepath.Join(dir, tmpDirName))
	require.NoError(t, err)
	require.Len(t, entries, 1, "the failed upload left nothing behind")
	assert.Equal(t, filepath.Base(first), entries[0].Name())

	// Over the size limit: refused and removed, whatever the request declared.
	m.opts.MaxBackupBytes = 1 << 20
	m.opts.DiskFree = func(string) (int64, error) { return 1 << 40, nil }
	_, err = m.SaveUpload(io.LimitReader(zeroReader{}, 2<<20), 0)
	require.ErrorIs(t, err, ErrTooLarge)
	entries, err = os.ReadDir(filepath.Join(dir, tmpDirName))
	require.NoError(t, err)
	assert.Len(t, entries, 1)
}

type zeroReader struct{}

func (zeroReader) Read(p []byte) (int, error) {
	clear(p)
	return len(p), nil
}

func TestAcquireUploadIsExclusive(t *testing.T) {
	m := &Manager{}
	release, err := m.AcquireUpload()
	require.NoError(t, err)
	_, err = m.AcquireUpload()
	require.ErrorIs(t, err, ErrBusy)
	release()
	release, err = m.AcquireUpload()
	require.NoError(t, err)
	release()
}

// A restore check that runs out of disk while unpacking reports the space
// error, not a corrupt file, and carries the cause for translation.
func TestUnpackOutOfSpaceKeepsTheCause(t *testing.T) {
	e := newTestEnv(t)
	e.configure()
	b := e.backup(TriggerManual)
	// Enough for the download, the reserve, and the database already on
	// disk, but not for the unpacked database.
	used, _ := fakeDisk(t, e.dir, 0)(e.dir)
	e.diskFree = fakeDisk(t, e.dir, diskReserve-used+b.Size+1)
	e.advance(rateWindow)
	e.m = e.newManager("v1.2.3")
	res, err := e.prepare(b, testPassword)
	require.NoError(t, err)
	require.False(t, res.CanApply)
	c := lastCheck(res)
	assert.Equal(t, CheckPayload, c.Code)
	require.ErrorIs(t, c.Err, ErrInsufficientSpace)
}
