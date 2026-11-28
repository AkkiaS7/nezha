package singleton

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"

	"github.com/nezhahq/nezha/model"
	"github.com/nezhahq/nezha/pkg/backup"
)

// newHistoryCleanupTestDB points DB at a fresh file database (the size
// statistics need a file) with the service tables.
func newHistoryCleanupTestDB(t *testing.T) {
	t.Helper()
	originalDB, originalTSDB, originalBackup := DB, TSDBShared, BackupShared
	originalPause, originalDiskFree := historyCleanupPause, diskFreeFunc

	db, err := gorm.Open(sqlite.Open(filepath.Join(t.TempDir(), "sqlite.db")), &gorm.Config{})
	require.NoError(t, err)
	require.NoError(t, db.AutoMigrate(&model.Service{}, &model.ServiceHistory{}))
	sqlDB, err := db.DB()
	require.NoError(t, err)

	DB, TSDBShared, BackupShared = db, nil, nil
	historyCleanupPause = 0
	diskFreeFunc = func(string) int64 { return 0 }
	setHistoryJob(model.HistoryCleanupJob{State: model.HistoryCleanupIdle})
	t.Cleanup(func() {
		// A job still running would use the closed database.
		dbMaintenanceMu.Lock()
		dbMaintenanceMu.Unlock()
		_ = sqlDB.Close()
		DB, TSDBShared, BackupShared = originalDB, originalTSDB, originalBackup
		historyCleanupPause, diskFreeFunc = originalPause, originalDiskFree
		setHistoryJob(model.HistoryCleanupJob{State: model.HistoryCleanupIdle})
	})
}

func waitHistoryCleanup(t *testing.T) model.HistoryCleanupJob {
	t.Helper()
	var job model.HistoryCleanupJob
	require.Eventually(t, func() bool {
		job = currentHistoryJob()
		return job.State == model.HistoryCleanupSucceeded || job.State == model.HistoryCleanupFailed
	}, 30*time.Second, 10*time.Millisecond)
	// The worker releases the lock right after publishing the final state.
	dbMaintenanceMu.Lock()
	dbMaintenanceMu.Unlock()
	return job
}

type historyFixture struct {
	kept, deleted int
}

// seedHistory stores rows on both sides of every cleanup rule for a 7-day
// retention. Times are written by GORM the way the service sentinel writes them.
func seedHistory(t *testing.T, now time.Time, oldPingRows int) historyFixture {
	t.Helper()
	service := &model.Service{Name: "kept", Type: model.TaskTypeTCPPing, Target: "192.0.2.1:80"}
	require.NoError(t, DB.Create(service).Error)
	const deletedService = 999
	day := 24 * time.Hour
	rows := []model.ServiceHistory{
		// Per-server ping rows: kept for 7 days.
		{ServiceID: service.ID, ServerID: 1, CreatedAt: now.Add(-7*day + time.Hour)},
		{ServiceID: service.ID, ServerID: 1, CreatedAt: now.Add(-2 * day)},
		{ServiceID: service.ID, ServerID: 1, CreatedAt: now.Add(-7*day - time.Hour)},
		{ServiceID: service.ID, ServerID: 2, CreatedAt: now.Add(-40 * day)},
		// Summaries: kept for 30 days whatever the retention.
		{ServiceID: service.ID, CreatedAt: now.Add(-20 * day)},
		{ServiceID: service.ID, CreatedAt: now.Add(-30*day + time.Hour)},
		{ServiceID: service.ID, CreatedAt: now.Add(-30*day - time.Hour)},
		// History of a deleted service goes whatever its age.
		{ServiceID: deletedService, ServerID: 1, CreatedAt: now.Add(-time.Hour)},
		{ServiceID: deletedService, CreatedAt: now.Add(-time.Hour)},
	}
	f := historyFixture{kept: 4, deleted: 5}
	for i := 0; i < oldPingRows; i++ {
		rows = append(rows, model.ServiceHistory{ServiceID: service.ID, ServerID: 3, CreatedAt: now.Add(-10*day - time.Duration(i)*time.Second)})
	}
	f.deleted += oldPingRows
	require.NoError(t, DB.CreateInBatches(rows, 500).Error)
	return f
}

func TestHistoryCleanupDeletesByRetentionRules(t *testing.T) {
	newHistoryCleanupTestDB(t)
	// More rows than a batch, so the delete loop runs several times.
	f := seedHistory(t, time.Now(), 2*historyCleanupBatchSize+10)
	total := int64(f.kept + f.deleted)

	usage, err := GetDatabaseUsage(context.Background(), 7)
	require.NoError(t, err)
	require.True(t, usage.HistoryAvailable)
	require.Equal(t, total, usage.HistoryRows)
	require.NotEmpty(t, usage.HistoryOldestAt)
	require.Positive(t, usage.DBSize)
	require.Equal(t, model.HistoryCleanupIdle, usage.Job.State)
	require.NotNil(t, usage.Estimate)
	require.Equal(t, 7, usage.Estimate.RetentionDays)
	require.Equal(t, int64(f.deleted), usage.Estimate.DeletableRows)
	require.False(t, usage.Estimate.Approximate)
	require.Positive(t, usage.Estimate.EstimatedFreeSize)
	require.Less(t, usage.Estimate.EstimatedFreeSize, usage.DBSize)
	require.Equal(t, 2*usage.DBSize-usage.Estimate.EstimatedFreeSize, usage.Estimate.VacuumRequiredSize)

	job, err := StartHistoryCleanup(7, model.HistoryVacuumModeOnline)
	require.NoError(t, err)
	require.Equal(t, model.HistoryCleanupDeleting, job.State)
	require.NotEmpty(t, job.StartedAt)

	job = waitHistoryCleanup(t)
	require.Equal(t, model.HistoryCleanupSucceeded, job.State, job.Error)
	require.Equal(t, int64(f.deleted), job.DeletedRows)
	require.Empty(t, job.VacuumSkipped)
	require.Positive(t, job.FreedSize)
	require.NotEmpty(t, job.FinishedAt)

	var left []model.ServiceHistory
	require.NoError(t, DB.Order("created_at").Find(&left).Error)
	require.Len(t, left, f.kept)
	now := time.Now()
	for _, row := range left {
		require.NotEqual(t, uint64(999), row.ServiceID)
		if row.ServerID == 0 {
			require.True(t, row.CreatedAt.After(now.AddDate(0, 0, -30)), row.CreatedAt)
		} else {
			require.True(t, row.CreatedAt.After(now.AddDate(0, 0, -7)), row.CreatedAt)
		}
	}

	usage, err = GetDatabaseUsage(context.Background(), 7)
	require.NoError(t, err)
	require.Zero(t, usage.Estimate.DeletableRows)
	require.Equal(t, job, usage.Job)
}

func TestHistoryCleanupLongRetentionKeepsSummariesOfThatAge(t *testing.T) {
	newHistoryCleanupTestDB(t)
	service := &model.Service{Name: "kept", Type: model.TaskTypeTCPPing, Target: "192.0.2.1:80"}
	require.NoError(t, DB.Create(service).Error)
	now := time.Now()
	require.NoError(t, DB.Create(&[]model.ServiceHistory{
		{ServiceID: service.ID, CreatedAt: now.AddDate(0, 0, -60)},
		{ServiceID: service.ID, ServerID: 1, CreatedAt: now.AddDate(0, 0, -60)},
		{ServiceID: service.ID, CreatedAt: now.AddDate(0, 0, -100)},
		{ServiceID: service.ID, ServerID: 1, CreatedAt: now.AddDate(0, 0, -100)},
	}).Error)

	_, err := StartHistoryCleanup(90, model.HistoryVacuumModeNone)
	require.NoError(t, err)
	job := waitHistoryCleanup(t)
	require.Equal(t, model.HistoryCleanupSucceeded, job.State, job.Error)
	require.Equal(t, int64(2), job.DeletedRows)
	require.Equal(t, model.HistoryVacuumNotRequested, job.VacuumSkipped)

	var left int64
	require.NoError(t, DB.Model(&model.ServiceHistory{}).Count(&left).Error)
	require.Equal(t, int64(2), left)
}

func TestHistoryCleanupSkipsVacuumWithoutDiskSpace(t *testing.T) {
	newHistoryCleanupTestDB(t)
	f := seedHistory(t, time.Now(), 0)
	diskFreeFunc = func(string) int64 { return 1 }

	_, err := StartHistoryCleanup(7, model.HistoryVacuumModeOnline)
	require.NoError(t, err)
	job := waitHistoryCleanup(t)
	require.Equal(t, model.HistoryCleanupSucceeded, job.State, job.Error)
	require.Equal(t, int64(f.deleted), job.DeletedRows)
	require.Equal(t, model.HistoryVacuumInsufficientDisk, job.VacuumSkipped)

	usage, err := GetDatabaseUsage(context.Background(), 0)
	require.NoError(t, err)
	require.Equal(t, int64(1), usage.DiskFree)
	require.Nil(t, usage.Estimate)
}

func TestHistoryCleanupRejectsConcurrentWork(t *testing.T) {
	newHistoryCleanupTestDB(t)

	dbMaintenanceMu.Lock()
	_, err := StartHistoryCleanup(30, model.HistoryVacuumModeOnline)
	require.ErrorIs(t, err, ErrDatabaseBusy)
	require.ErrorIs(t, PerformMaintenance(), ErrDatabaseBusy)
	dbMaintenanceMu.Unlock()
	require.Equal(t, model.HistoryCleanupIdle, currentHistoryJob().State)

	require.NoError(t, PerformMaintenance())
}

func TestHistoryCleanupValidatesRequest(t *testing.T) {
	newHistoryCleanupTestDB(t)
	for _, days := range []int{-1, 0, model.HistoryCleanupMaxRetentionDays + 1} {
		_, err := StartHistoryCleanup(days, model.HistoryVacuumModeOnline)
		require.ErrorIs(t, err, ErrInvalidRetentionDays, days)
	}
	require.True(t, ValidHistoryRetentionDays(model.HistoryCleanupMinRetentionDays))
	require.True(t, ValidHistoryRetentionDays(model.HistoryCleanupMaxRetentionDays))
}

func TestHistoryCleanupUnavailableWithoutHistoryTable(t *testing.T) {
	newHistoryCleanupTestDB(t)
	// TSDB mode drops the table.
	require.NoError(t, DB.Migrator().DropTable(&model.ServiceHistory{}))

	usage, err := GetDatabaseUsage(context.Background(), 30)
	require.NoError(t, err)
	require.False(t, usage.HistoryAvailable)
	require.Positive(t, usage.DBSize)
	require.Nil(t, usage.Estimate)

	_, err = StartHistoryCleanup(30, model.HistoryVacuumModeOnline)
	require.ErrorIs(t, err, ErrServiceHistoryUnavailable)
}

func TestApproximateDeletableRows(t *testing.T) {
	newHistoryCleanupTestDB(t)
	now := time.Now()
	var rows []model.ServiceHistory
	for i := 10; i > 0; i-- {
		rows = append(rows, model.ServiceHistory{ServiceID: 1, ServerID: 1, CreatedAt: now.AddDate(0, 0, -i)})
	}
	require.NoError(t, DB.Create(&rows).Error)

	n, err := approximateDeletableRows(context.Background(), now.AddDate(0, 0, -3).Add(-time.Hour), 10)
	require.NoError(t, err)
	require.Equal(t, int64(7), n)

	n, err = approximateDeletableRows(context.Background(), now.Add(time.Hour), 10)
	require.NoError(t, err)
	require.Equal(t, int64(10), n)
}

// created_at keeps the writer's UTC offset, which changes with the process
// time zone. Rows on both sides of each cutoff, written with offsets whose
// text sorts the other way, must be judged by the instant.
func TestHistoryCleanupComparesInstantsAcrossUTCOffsets(t *testing.T) {
	newHistoryCleanupTestDB(t)
	service := &model.Service{Name: "kept", Type: model.TaskTypeTCPPing, Target: "192.0.2.1:80"}
	require.NoError(t, DB.Create(service).Error)
	east := time.FixedZone("UTC+14", 14*3600)
	west := time.FixedZone("UTC-12", -12*3600)
	now := time.Now()
	pingCut := now.AddDate(0, 0, -7)
	summaryCut := now.AddDate(0, 0, -30)
	type row struct {
		serverID uint64
		at       time.Time
		kept     bool
	}
	rows := []row{
		// The text of these sorts before the cutoff, yet they are newer.
		{1, pingCut.Add(30 * time.Minute).In(west), true},
		{0, summaryCut.Add(30 * time.Minute).In(west), true},
		// The text of these sorts after the cutoff, yet they are older.
		{1, pingCut.Add(-30 * time.Minute).In(east), false},
		{0, summaryCut.Add(-30 * time.Minute).In(east), false},
		// Same offset on both sides.
		{1, pingCut.Add(30 * time.Minute).In(east), true},
		{1, pingCut.Add(-30 * time.Minute).In(west), false},
		{0, summaryCut.Add(30 * time.Minute).In(east), true},
		{0, summaryCut.Add(-30 * time.Minute).In(west), false},
	}
	for _, r := range rows {
		require.NoError(t, DB.Create(&model.ServiceHistory{ServiceID: service.ID, ServerID: r.serverID, CreatedAt: r.at}).Error)
	}
	var stored []string
	require.NoError(t, DB.Raw("SELECT created_at FROM service_histories WHERE id IN (1, 3)").Scan(&stored).Error)
	require.Len(t, stored, 2)
	require.Contains(t, stored[0], "-12:00", "the fixture must store the writer's offset")
	require.Contains(t, stored[1], "+14:00", "the fixture must store the writer's offset")

	usage, err := GetDatabaseUsage(context.Background(), 7)
	require.NoError(t, err)
	require.Equal(t, int64(4), usage.Estimate.DeletableRows)

	_, err = StartHistoryCleanup(7, model.HistoryVacuumModeNone)
	require.NoError(t, err)
	job := waitHistoryCleanup(t)
	require.Equal(t, model.HistoryCleanupSucceeded, job.State, job.Error)
	require.Equal(t, int64(4), job.DeletedRows)

	var left []model.ServiceHistory
	require.NoError(t, DB.Order("id").Find(&left).Error)
	var keptIDs []uint64
	for _, r := range left {
		keptIDs = append(keptIDs, r.ID)
	}
	var want []uint64
	for i, r := range rows {
		if r.kept {
			want = append(want, uint64(i+1))
		}
	}
	require.Equal(t, want, keptIDs)
}

// The backup manager shares the cleanup's lock, so that a restore and a
// cleanup exclude each other (the order of starting is tested in pkg/backup).
func TestBackupOptionsShareDatabaseLock(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "sqlite.db")
	opts := backupOptions("config.yaml", dbPath)
	require.Same(t, &dbMaintenanceMu, opts.DatabaseLock)
	// The settings the backup manager had before the lock was added are kept.
	require.Equal(t, dbPath, opts.DatabasePath)
	require.Equal(t, filepath.Dir(dbPath), opts.DataDir)
	require.Equal(t, backup.MaxBytesFromEnv(), opts.MaxBackupBytes)
	require.NotNil(t, opts.Restart)

	newHistoryCleanupTestDB(t)
	require.True(t, opts.DatabaseLock.TryLock()) // as Apply does
	_, err := StartHistoryCleanup(30, model.HistoryVacuumModeOnline)
	require.ErrorIs(t, err, ErrDatabaseBusy)
	require.ErrorIs(t, PerformMaintenance(), ErrDatabaseBusy)
	opts.DatabaseLock.Unlock()
}
