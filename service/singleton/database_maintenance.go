package singleton

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/nezhahq/nezha/model"
	"github.com/nezhahq/nezha/pkg/backup"
)

var (
	ErrDatabaseBusy              = errors.New("another database cleanup or maintenance is in progress")
	ErrServiceHistoryUnavailable = errors.New("service history is not stored in the SQLite database")
	ErrInvalidRetentionDays      = errors.New("invalid retention days")
)

const (
	// A batch of this size holds SQLite's write lock for some ten
	// milliseconds on a million-row table, short enough for the report
	// writers waiting behind it (busy_timeout is 5 s).
	historyCleanupBatchSize = 5000
	historyEstimateTimeout  = 3 * time.Second
)

// historyCleanupPause is the gap between two delete batches in which other
// writers take the lock. Tests shorten it.
var historyCleanupPause = 50 * time.Millisecond

// diskFreeFunc is replaced by tests.
var diskFreeFunc = diskFree

// dbMaintenanceMu is held by the operations that rewrite large parts of the
// database: the history cleanup and VACUUM (PerformMaintenance). It is only
// taken with TryLock, so a request never waits behind a long-running one.
var dbMaintenanceMu sync.Mutex

var (
	historyJobMu sync.Mutex
	historyJob   = model.HistoryCleanupJob{State: model.HistoryCleanupIdle}
)

// historyCleanupCondition selects the rows a cleanup with the given cutoffs
// removes: per-server ping rows older than the retention, 15-minute summaries
// older than max(retention, 30 days), and the rows of deleted services.
//
// created_at is stored as text with the writer's UTC offset, and the cutoffs
// are bound the same way (time.Now() in the process time zone), so the text
// comparison matches the time comparison and can use the created_at index.
const historyCleanupCondition = "(server_id != 0 AND created_at < ?) OR (server_id = 0 AND created_at < ?) OR service_id NOT IN (SELECT id FROM services)"

func historyCutoffs(now time.Time, days int) (time.Time, time.Time) {
	return now.AddDate(0, 0, -days), now.AddDate(0, 0, -max(days, model.HistorySummaryMinRetentionDays))
}

// ValidHistoryRetentionDays reports whether days is an accepted retention.
func ValidHistoryRetentionDays(days int) bool {
	return days >= model.HistoryCleanupMinRetentionDays && days <= model.HistoryCleanupMaxRetentionDays
}

func serviceHistoryAvailable() bool {
	return DB != nil && !TSDBEnabled() && DB.Migrator().HasTable(&model.ServiceHistory{})
}

type dbFileStats struct {
	path      string
	pageSize  int64
	pageCount int64
	freePages int64
	walSize   int64
}

func (s dbFileStats) size() int64     { return s.pageCount*s.pageSize + s.walSize }
func (s dbFileStats) freeSize() int64 { return s.freePages * s.pageSize }
func (s dbFileStats) usedSize() int64 { return (s.pageCount - s.freePages) * s.pageSize }

func readDBFileStats(ctx context.Context) (dbFileStats, error) {
	var s dbFileStats
	db := DB.WithContext(ctx)
	for _, p := range []struct {
		pragma string
		dst    *int64
	}{{"page_size", &s.pageSize}, {"page_count", &s.pageCount}, {"freelist_count", &s.freePages}} {
		if err := db.Raw("PRAGMA " + p.pragma).Row().Scan(p.dst); err != nil {
			return s, err
		}
	}
	rows, err := db.Raw("PRAGMA database_list").Rows()
	if err != nil {
		return s, err
	}
	defer rows.Close()
	for rows.Next() {
		var seq int
		var name, file string
		if err := rows.Scan(&seq, &name, &file); err != nil {
			return s, err
		}
		if name == "main" {
			s.path = file
		}
	}
	if err := rows.Err(); err != nil {
		return s, err
	}
	if s.path != "" {
		if st, err := os.Stat(s.path + "-wal"); err == nil {
			s.walSize = st.Size()
		}
	}
	return s, nil
}

func (s dbFileStats) diskFree() int64 {
	if s.path == "" {
		return 0
	}
	return diskFreeFunc(filepath.Dir(s.path))
}

func formatJobTime(t time.Time) string {
	if Loc != nil {
		t = t.In(Loc)
	}
	return t.Format(time.RFC3339)
}

// GetDatabaseUsage reports the database size and the service history
// cleanup state. With retentionDays > 0 it also estimates what a cleanup with
// that retention would remove.
func GetDatabaseUsage(ctx context.Context, retentionDays int) (*model.DatabaseUsage, error) {
	st, err := readDBFileStats(ctx)
	if err != nil {
		return nil, err
	}
	u := &model.DatabaseUsage{
		DBSize:        st.size(),
		FreePagesSize: st.freeSize(),
		DiskFree:      st.diskFree(),
		Job:           currentHistoryJob(),
	}
	if !serviceHistoryAvailable() {
		return u, nil
	}
	u.HistoryAvailable = true
	db := DB.WithContext(ctx)
	if err := db.Model(&model.ServiceHistory{}).Count(&u.HistoryRows).Error; err != nil {
		return nil, err
	}
	var oldest sql.NullTime
	err = db.Raw("SELECT created_at FROM service_histories ORDER BY created_at LIMIT 1").Row().Scan(&oldest)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}
	if oldest.Valid {
		u.HistoryOldestAt = formatJobTime(oldest.Time)
	}
	if retentionDays > 0 {
		e, err := estimateHistoryCleanup(ctx, retentionDays, st, u.HistoryRows)
		if err != nil {
			return nil, err
		}
		u.Estimate = e
	}
	return u, nil
}

// estimateHistoryCleanup counts the rows a cleanup would delete and scales
// the space they take by their share of the rows; other tables are small next
// to service history. Measured against cleanup + VACUUM this is within about
// 10%, slightly high because pages are not full.
func estimateHistoryCleanup(ctx context.Context, days int, st dbFileStats, total int64) (*model.HistoryCleanupEstimate, error) {
	e := &model.HistoryCleanupEstimate{RetentionDays: days}
	cut, summaryCut := historyCutoffs(time.Now(), days)
	countCtx, cancel := context.WithTimeout(ctx, historyEstimateTimeout)
	err := DB.WithContext(countCtx).Model(&model.ServiceHistory{}).
		Where(historyCleanupCondition, cut, summaryCut).Count(&e.DeletableRows).Error
	cancel()
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		// The count scans the deleted rows; on a slow disk fall back to
		// the ids, which grow with created_at, around the cutoff.
		log.Printf("NEZHA>> service history estimate: exact count failed, approximating: %v", err)
		e.DeletableRows, err = approximateDeletableRows(ctx, cut, total)
		if err != nil {
			return nil, err
		}
		e.Approximate = true
	}
	if total > 0 {
		e.EstimatedFreeSize = int64(float64(st.usedSize()) * float64(e.DeletableRows) / float64(total))
	}
	e.EstimatedFreeSize += st.freeSize() + st.walSize
	after := max(st.size()-e.EstimatedFreeSize, 0)
	// VACUUM writes the compacted copy to a temporary file and keeps the
	// overwritten pages in the rollback journal until it commits.
	e.VacuumRequiredSize = st.size() + after
	return e, nil
}

func approximateDeletableRows(ctx context.Context, cut time.Time, total int64) (int64, error) {
	db := DB.WithContext(ctx)
	var minID, firstKept sql.NullInt64
	if err := db.Raw("SELECT MIN(id) FROM service_histories").Row().Scan(&minID); err != nil {
		return 0, err
	}
	err := db.Raw("SELECT id FROM service_histories WHERE created_at >= ? ORDER BY created_at LIMIT 1", cut).Row().Scan(&firstKept)
	if errors.Is(err, sql.ErrNoRows) {
		return total, nil
	}
	if err != nil {
		return 0, err
	}
	if !minID.Valid || !firstKept.Valid {
		return 0, nil
	}
	return min(max(firstKept.Int64-minID.Int64, 0), total), nil
}

func currentHistoryJob() model.HistoryCleanupJob {
	historyJobMu.Lock()
	defer historyJobMu.Unlock()
	return historyJob
}

func setHistoryJob(job model.HistoryCleanupJob) {
	historyJobMu.Lock()
	historyJob = job
	historyJobMu.Unlock()
}

// StartHistoryCleanup starts deleting service history older than
// retentionDays in the background, and compacts the database afterwards when
// vacuum is set. Only one cleanup or maintenance runs at a time.
func StartHistoryCleanup(retentionDays int, vacuum bool) (model.HistoryCleanupJob, error) {
	if !ValidHistoryRetentionDays(retentionDays) {
		return model.HistoryCleanupJob{}, ErrInvalidRetentionDays
	}
	if !serviceHistoryAvailable() {
		return model.HistoryCleanupJob{}, ErrServiceHistoryUnavailable
	}
	// A restore is applied by restarting; the database is about to be replaced.
	if BackupShared != nil && BackupShared.Restarting() {
		return model.HistoryCleanupJob{}, backup.ErrRestarting
	}
	if !dbMaintenanceMu.TryLock() {
		return model.HistoryCleanupJob{}, ErrDatabaseBusy
	}
	job := model.HistoryCleanupJob{
		State:         model.HistoryCleanupDeleting,
		RetentionDays: retentionDays,
		Vacuum:        vacuum,
		StartedAt:     formatJobTime(time.Now()),
	}
	if !vacuum {
		job.VacuumSkipped = model.HistoryVacuumNotRequested
	}
	setHistoryJob(job)
	go func() {
		defer dbMaintenanceMu.Unlock()
		runHistoryCleanup(job)
	}()
	return job, nil
}

func runHistoryCleanup(job model.HistoryCleanupJob) {
	ctx := context.Background()
	fail := func(err error) {
		log.Printf("NEZHA>> service history cleanup failed after deleting %d rows: %v", job.DeletedRows, err)
		job.State = model.HistoryCleanupFailed
		job.Error = err.Error()
		job.FinishedAt = formatJobTime(time.Now())
		setHistoryJob(job)
	}
	before, err := readDBFileStats(ctx)
	if err != nil {
		fail(err)
		return
	}
	cut, summaryCut := historyCutoffs(time.Now(), job.RetentionDays)
	for _, step := range []struct {
		cond string
		args []any
	}{
		{"server_id != 0 AND created_at < ?", []any{cut}},
		{"server_id = 0 AND created_at < ?", []any{summaryCut}},
		{"service_id NOT IN (SELECT id FROM services)", nil},
	} {
		query := "DELETE FROM service_histories WHERE id IN (SELECT id FROM service_histories WHERE " + step.cond + " LIMIT ?)"
		args := append(step.args, historyCleanupBatchSize)
		for {
			res := DB.Exec(query, args...)
			if res.Error != nil {
				fail(res.Error)
				return
			}
			if res.RowsAffected == 0 {
				break
			}
			job.DeletedRows += res.RowsAffected
			setHistoryJob(job)
			time.Sleep(historyCleanupPause)
		}
	}

	if job.Vacuum {
		st, err := readDBFileStats(ctx)
		if err != nil {
			fail(err)
			return
		}
		if !vacuumHasSpace(st) {
			job.VacuumSkipped = model.HistoryVacuumInsufficientDisk
		} else {
			job.State = model.HistoryCleanupVacuuming
			setHistoryJob(job)
			if err := DB.Exec("VACUUM").Error; err != nil {
				fail(fmt.Errorf("deleted %d rows, but VACUUM failed: %w", job.DeletedRows, err))
				return
			}
		}
	}

	if after, err := readDBFileStats(ctx); err == nil {
		job.FreedSize = max(before.size()-after.size(), 0)
	}
	job.State = model.HistoryCleanupSucceeded
	job.FinishedAt = formatJobTime(time.Now())
	setHistoryJob(job)
	log.Printf("NEZHA>> service history cleanup: deleted %d rows older than %d days, freed %d bytes", job.DeletedRows, job.RetentionDays, job.FreedSize)
}

// vacuumHasSpace reports whether VACUUM can run: it writes the compacted copy
// (the used pages) to SQLite's temporary directory, and the rollback journal
// next to the database can grow to the size of the database. An unknown free
// space (0) is not checked.
func vacuumHasSpace(st dbFileStats) bool {
	if free := st.diskFree(); free > 0 && free < st.size()+st.usedSize() {
		return false
	}
	if free := diskFreeFunc(os.TempDir()); free > 0 && free < st.usedSize() {
		return false
	}
	return true
}
