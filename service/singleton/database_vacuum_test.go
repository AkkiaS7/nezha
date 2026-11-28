package singleton

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"

	"github.com/nezhahq/nezha/model"
)

func TestRecommendVacuum(t *testing.T) {
	const mib = 1 << 20
	for _, c := range []struct {
		sizeAfter    int64
		restart      bool
		seconds      int
		mode, reason string
	}{
		{0, true, 1, model.HistoryVacuumModeOnline, model.HistoryVacuumReasonShort},
		{50 * mib, true, 1, model.HistoryVacuumModeOnline, model.HistoryVacuumReasonShort},
		{204 * mib, true, 3, model.HistoryVacuumModeOnline, model.HistoryVacuumReasonShort},
		{220 * mib, true, 4, model.HistoryVacuumModeRestart, model.HistoryVacuumReasonLong},
		{2 << 30, true, 30, model.HistoryVacuumModeRestart, model.HistoryVacuumReasonLong},
		{2 << 30, false, 30, model.HistoryVacuumModeOnline, model.HistoryVacuumReasonRestartUnavailable},
		{50 * mib, false, 1, model.HistoryVacuumModeOnline, model.HistoryVacuumReasonShort},
	} {
		e := &model.HistoryCleanupEstimate{SizeAfter: c.sizeAfter}
		recommendVacuum(e, c.restart)
		require.Equal(t, c.seconds, e.OnlineVacuumSeconds, c)
		require.Equal(t, c.seconds+restartStartupAllowanceSeconds, e.RestartDowntimeSeconds, c)
		require.Equal(t, c.mode, e.RecommendedVacuumMode, c)
		require.Equal(t, c.reason, e.RecommendationReason, c)
	}
}

// withRestart makes the dashboard look supervised and records restarts.
func withRestart(t *testing.T, supervised bool) *atomic.Int32 {
	t.Helper()
	var restarts atomic.Int32
	originalRequest, originalSupervised, originalDelay := RequestRestart, restartSupervised, restartAnnounceDelay
	RequestRestart = func() { restarts.Add(1) }
	restartSupervised = func() bool { return supervised }
	restartAnnounceDelay = 0
	t.Cleanup(func() {
		RequestRestart, restartSupervised, restartAnnounceDelay = originalRequest, originalSupervised, originalDelay
	})
	return &restarts
}

func testDBPath(t *testing.T) string {
	t.Helper()
	st, err := readDBFileStats(context.Background())
	require.NoError(t, err)
	require.NotEmpty(t, st.path)
	return st.path
}

func TestUsageReportsRestartAvailability(t *testing.T) {
	newHistoryCleanupTestDB(t)
	seedHistory(t, time.Now(), 0)

	withRestart(t, false)
	u, err := GetDatabaseUsage(context.Background(), 7)
	require.NoError(t, err)
	require.False(t, u.RestartAvailable)
	require.Equal(t, u.DBSize-u.Estimate.EstimatedFreeSize, u.Estimate.SizeAfter)
	require.Equal(t, model.HistoryVacuumModeOnline, u.Estimate.RecommendedVacuumMode)
	_, err = StartHistoryCleanup(7, model.HistoryVacuumModeRestart)
	require.ErrorIs(t, err, ErrRestartUnavailable)

	withRestart(t, true)
	u, err = GetDatabaseUsage(context.Background(), 7)
	require.NoError(t, err)
	require.True(t, u.RestartAvailable)

	_, err = StartHistoryCleanup(7, "later")
	require.ErrorIs(t, err, ErrInvalidVacuumMode)
}

// The whole compaction while restarting: the cleanup deletes, records the
// request and restarts; the next start compacts before serving and reports
// the result as the current job; a further start does not compact again.
func TestVacuumOnRestart(t *testing.T) {
	newHistoryCleanupTestDB(t)
	f := seedHistory(t, time.Now(), 0)
	restarts := withRestart(t, true)
	path := testDBPath(t)
	var vacuums atomic.Int32
	originalExec := execVacuum
	execVacuum = func(ctx context.Context) error { vacuums.Add(1); return originalExec(ctx) }
	t.Cleanup(func() { execVacuum = originalExec })

	job, err := StartHistoryCleanup(7, model.HistoryVacuumModeRestart)
	require.NoError(t, err)
	require.Equal(t, model.HistoryVacuumModeRestart, job.VacuumMode)
	require.True(t, job.Vacuum)
	require.Eventually(t, func() bool { return restarts.Load() == 1 }, 5*time.Second, 5*time.Millisecond)
	job = currentHistoryJob()
	require.Equal(t, model.HistoryCleanupRestarting, job.State)
	require.Equal(t, int64(f.deleted), job.DeletedRows)
	require.Zero(t, vacuums.Load())
	// The lock is kept until the process exits.
	require.False(t, dbMaintenanceMu.TryLock())
	_, err = os.Stat(vacuumMarkerPath(path, vacuumPendingFile))
	require.NoError(t, err)

	// The process restarts.
	dbMaintenanceMu.Unlock()
	setHistoryJob(model.HistoryCleanupJob{State: model.HistoryCleanupIdle})
	RunPendingVacuum()
	require.Equal(t, int32(1), vacuums.Load())
	job = currentHistoryJob()
	require.Equal(t, model.HistoryCleanupSucceeded, job.State, job.Error)
	require.Equal(t, model.HistoryVacuumModeRestart, job.VacuumMode)
	require.Equal(t, int64(f.deleted), job.DeletedRows)
	require.Empty(t, job.VacuumSkipped)
	require.NotEmpty(t, job.FinishedAt)
	for _, name := range []string{vacuumPendingFile, vacuumRunningFile} {
		_, err := os.Stat(vacuumMarkerPath(path, name))
		require.ErrorIs(t, err, os.ErrNotExist, name)
	}

	// A later start reports the same result and does not compact again.
	setHistoryJob(model.HistoryCleanupJob{State: model.HistoryCleanupIdle})
	RunPendingVacuum()
	require.Equal(t, int32(1), vacuums.Load())
	require.Equal(t, job, currentHistoryJob())

	// The next cleanup replaces the result.
	_, err = StartHistoryCleanup(7, model.HistoryVacuumModeNone)
	require.NoError(t, err)
	waitHistoryCleanup(t)
	_, err = os.Stat(vacuumMarkerPath(path, vacuumResultFile))
	require.ErrorIs(t, err, os.ErrNotExist)
}

// A run that stopped during VACUUM is reported as failed and not repeated,
// so it cannot restart the dashboard forever.
func TestVacuumOnRestartInterrupted(t *testing.T) {
	newHistoryCleanupTestDB(t)
	path := testDBPath(t)
	var vacuums atomic.Int32
	originalExec := execVacuum
	execVacuum = func(ctx context.Context) error { vacuums.Add(1); return originalExec(ctx) }
	t.Cleanup(func() { execVacuum = originalExec })

	require.NoError(t, writeVacuumMarker(vacuumMarkerPath(path, vacuumRunningFile), vacuumMarker{
		Job: model.HistoryCleanupJob{State: model.HistoryCleanupRestarting, VacuumMode: model.HistoryVacuumModeRestart, Vacuum: true, DeletedRows: 42},
	}))
	RunPendingVacuum()
	require.Zero(t, vacuums.Load())
	job := currentHistoryJob()
	require.Equal(t, model.HistoryCleanupFailed, job.State)
	require.Contains(t, job.Error, "did not finish")
	require.Equal(t, int64(42), job.DeletedRows)
	_, err := os.Stat(vacuumMarkerPath(path, vacuumRunningFile))
	require.ErrorIs(t, err, os.ErrNotExist)
}

// A failing VACUUM or a full disk at startup is reported; the start goes on.
func TestVacuumOnRestartFailureAndDiskFull(t *testing.T) {
	newHistoryCleanupTestDB(t)
	path := testDBPath(t)
	pending := vacuumMarker{Job: model.HistoryCleanupJob{State: model.HistoryCleanupRestarting, VacuumMode: model.HistoryVacuumModeRestart, Vacuum: true, DeletedRows: 7}}

	originalExec := execVacuum
	execVacuum = func(context.Context) error { return errors.New("injected VACUUM failure") }
	t.Cleanup(func() { execVacuum = originalExec })
	require.NoError(t, writeVacuumMarker(vacuumMarkerPath(path, vacuumPendingFile), pending))
	RunPendingVacuum()
	job := currentHistoryJob()
	require.Equal(t, model.HistoryCleanupFailed, job.State)
	require.Contains(t, job.Error, "injected VACUUM failure")

	execVacuum = originalExec
	diskFreeFunc = func(string) int64 { return 1 }
	require.NoError(t, writeVacuumMarker(vacuumMarkerPath(path, vacuumPendingFile), pending))
	RunPendingVacuum()
	job = currentHistoryJob()
	require.Equal(t, model.HistoryCleanupSucceeded, job.State)
	require.Equal(t, model.HistoryVacuumInsufficientDisk, job.VacuumSkipped)
}

func TestHistoryCleanupRequestCompatibility(t *testing.T) {
	require.Equal(t, model.HistoryVacuumModeOnline, model.HistoryCleanupRequest{Vacuum: true}.VacuumModeOrDefault())
	require.Equal(t, model.HistoryVacuumModeNone, model.HistoryCleanupRequest{}.VacuumModeOrDefault())
	require.Equal(t, model.HistoryVacuumModeRestart, model.HistoryCleanupRequest{Vacuum: true, VacuumMode: model.HistoryVacuumModeRestart}.VacuumModeOrDefault())
	require.Equal(t, model.HistoryVacuumModeNone, model.HistoryCleanupRequest{Vacuum: true, VacuumMode: model.HistoryVacuumModeNone}.VacuumModeOrDefault())
}

// openTestDB points DB at dsn for the rest of the test.
func openTestDB(t *testing.T, dsn string) {
	t.Helper()
	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{})
	require.NoError(t, err)
	require.NoError(t, db.AutoMigrate(&model.Service{}, &model.ServiceHistory{}))
	sqlDB, err := db.DB()
	require.NoError(t, err)
	t.Cleanup(func() { _ = sqlDB.Close() })
	DB = db
}

// A database opened through a file: URI is found by the request and by the
// startup under the same path, so its compaction runs.
func TestVacuumOnRestartWithFileURI(t *testing.T) {
	newHistoryCleanupTestDB(t)
	path := filepath.Join(t.TempDir(), "uri.db")
	openTestDB(t, "file:"+path+"?_busy_timeout=5000")
	f := seedHistory(t, time.Now(), 0)
	restarts := withRestart(t, true)
	// SQLite reports the resolved path (on macOS /private/var for /var):
	// compare the file, not the string, and use what SQLite reports below.
	reported := testDBPath(t)
	want, err := os.Stat(path)
	require.NoError(t, err)
	got, err := os.Stat(reported)
	require.NoError(t, err)
	require.True(t, os.SameFile(want, got), "%s is not %s", reported, path)
	u, err := GetDatabaseUsage(context.Background(), 7)
	require.NoError(t, err)
	require.True(t, u.RestartAvailable)

	_, err = StartHistoryCleanup(7, model.HistoryVacuumModeRestart)
	require.NoError(t, err)
	require.Eventually(t, func() bool { return restarts.Load() == 1 }, 5*time.Second, 5*time.Millisecond)
	_, err = os.Stat(reported + vacuumPendingFile)
	require.NoError(t, err)

	dbMaintenanceMu.Unlock()
	setHistoryJob(model.HistoryCleanupJob{State: model.HistoryCleanupIdle})
	RunPendingVacuum()
	job := currentHistoryJob()
	require.Equal(t, model.HistoryCleanupSucceeded, job.State, job.Error)
	require.Equal(t, int64(f.deleted), job.DeletedRows)
	_, err = os.Stat(reported + vacuumPendingFile)
	require.ErrorIs(t, err, os.ErrNotExist)
}

// Two databases in one directory keep their own markers: one never runs
// or reports the other's compaction.
func TestVacuumMarkersPerDatabase(t *testing.T) {
	newHistoryCleanupTestDB(t)
	dir := t.TempDir()
	a, b := filepath.Join(dir, "a.db"), filepath.Join(dir, "b.db")
	var vacuums atomic.Int32
	originalExec := execVacuum
	execVacuum = func(ctx context.Context) error { vacuums.Add(1); return originalExec(ctx) }
	t.Cleanup(func() { execVacuum = originalExec })
	pending := vacuumMarker{Job: model.HistoryCleanupJob{State: model.HistoryCleanupRestarting, VacuumMode: model.HistoryVacuumModeRestart, Vacuum: true, DeletedRows: 5}}

	openTestDB(t, a)
	require.NoError(t, writeVacuumMarker(vacuumMarkerPath(a, vacuumPendingFile), pending))

	openTestDB(t, b)
	RunPendingVacuum()
	require.Zero(t, vacuums.Load())
	require.Equal(t, model.HistoryCleanupIdle, currentHistoryJob().State)
	_, err := os.Stat(a + vacuumPendingFile)
	require.NoError(t, err, "b must not take a's marker")

	openTestDB(t, a)
	RunPendingVacuum()
	require.Equal(t, int32(1), vacuums.Load())
	require.Equal(t, model.HistoryCleanupSucceeded, currentHistoryJob().State)
	_, err = os.Stat(a + vacuumResultFile)
	require.NoError(t, err)

	// b does not show a's result.
	setHistoryJob(model.HistoryCleanupJob{State: model.HistoryCleanupIdle})
	openTestDB(t, b)
	RunPendingVacuum()
	require.Equal(t, model.HistoryCleanupIdle, currentHistoryJob().State)
	require.Equal(t, int32(1), vacuums.Load())
}
