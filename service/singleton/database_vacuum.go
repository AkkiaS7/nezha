package singleton

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math"
	"os"
	"time"

	"github.com/nezhahq/nezha/model"
)

// VACUUM holds SQLite's exclusive lock for its whole run: every dashboard
// request (WAF, session checks, handlers) meets the lock, waits busy_timeout
// (5 s) and then fails. A long VACUUM is therefore better run while the
// dashboard restarts, before it opens HTTP and gRPC.
const (
	// vacuumSecondsPerGiB is the upper end of the measured cold-cache VACUUM
	// rate (11-15 s per GiB left in the database).
	vacuumSecondsPerGiB = 15
	// vacuumRestartThresholdSeconds: an online VACUUM expected to take longer
	// is not recommended. busy_timeout is 5 s; this keeps a 40% margin.
	vacuumRestartThresholdSeconds = 3
	// restartStartupAllowanceSeconds is added to the VACUUM time for the
	// shutdown and start of the dashboard.
	restartStartupAllowanceSeconds = 15
)

// Suffixes of the markers of a compaction while restarting, appended to the
// database file name (like SQLite's -journal), so each database file has
// its own.
const (
	vacuumPendingFile = ".vacuum-pending"
	vacuumRunningFile = ".vacuum-running"
	vacuumResultFile  = ".vacuum-result"
)

// Replaced by tests.
var (
	execVacuum = func(ctx context.Context) error {
		return DB.WithContext(ctx).Exec("VACUUM").Error
	}
	// restartSupervised reports whether a supervisor will start the
	// dashboard again after it exits: systemd sets INVOCATION_ID for its
	// services, and container runtimes leave these files.
	restartSupervised = func() bool {
		if os.Getenv("INVOCATION_ID") != "" {
			return true
		}
		for _, p := range []string{"/.dockerenv", "/run/.containerenv"} {
			if _, err := os.Stat(p); err == nil {
				return true
			}
		}
		return false
	}
	// restartAnnounceDelay keeps the restarting state visible to the admin
	// page's 2 s poll before the dashboard goes down.
	restartAnnounceDelay = 3 * time.Second
)

// restartAvailable reports whether a compaction while restarting can be
// requested for the database at dbPath: the dashboard can restart itself
// (the restore uses the same path) and the database is a file.
func restartAvailable(dbPath string) bool {
	return dbPath != "" && RequestRestart != nil && restartSupervised()
}

// vacuumMarkerPath returns the marker of the database file at dbPath, the
// path SQLite reports for the open database (PRAGMA database_list), which
// the requests and the startup both use.
func vacuumMarkerPath(dbPath, suffix string) string {
	return dbPath + suffix
}

// recommendVacuum fills the expected durations and the recommended mode of e.
func recommendVacuum(e *model.HistoryCleanupEstimate, restartAvailable bool) {
	seconds := int(math.Ceil(float64(e.SizeAfter) / (1 << 30) * vacuumSecondsPerGiB))
	e.OnlineVacuumSeconds = max(seconds, 1)
	e.RestartDowntimeSeconds = e.OnlineVacuumSeconds + restartStartupAllowanceSeconds
	switch {
	case e.OnlineVacuumSeconds <= vacuumRestartThresholdSeconds:
		e.RecommendedVacuumMode, e.RecommendationReason = model.HistoryVacuumModeOnline, model.HistoryVacuumReasonShort
	case restartAvailable:
		e.RecommendedVacuumMode, e.RecommendationReason = model.HistoryVacuumModeRestart, model.HistoryVacuumReasonLong
	default:
		e.RecommendedVacuumMode, e.RecommendationReason = model.HistoryVacuumModeOnline, model.HistoryVacuumReasonRestartUnavailable
	}
}

// vacuumMarker is the content of the pending, running and result markers.
type vacuumMarker struct {
	Job        model.HistoryCleanupJob `json:"job"`
	SizeBefore int64                   `json:"size_before"`
}

func writeVacuumMarker(path string, m vacuumMarker) error {
	data, err := json.Marshal(m)
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600) // #nosec G304 -- a fixed name next to the database
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return err
	}
	return os.Rename(tmp, path)
}

func readVacuumMarker(path string) (vacuumMarker, error) {
	var m vacuumMarker
	data, err := os.ReadFile(path) // #nosec G304 -- a fixed name next to the database
	if err != nil {
		return m, err
	}
	return m, json.Unmarshal(data, &m)
}

// requestVacuumOnRestart records the pending compaction and restarts the
// dashboard. The caller keeps dbMaintenanceMu, so nothing else rewrites the
// database before the process exits.
func requestVacuumOnRestart(job model.HistoryCleanupJob, dbPath string, sizeBefore int64) error {
	job.State = model.HistoryCleanupRestarting
	if err := writeVacuumMarker(vacuumMarkerPath(dbPath, vacuumPendingFile), vacuumMarker{Job: job, SizeBefore: sizeBefore}); err != nil {
		return err
	}
	setHistoryJob(job)
	log.Printf("NEZHA>> service history cleanup: deleted %d rows older than %d days, restarting to compact the database", job.DeletedRows, job.RetentionDays)
	time.Sleep(restartAnnounceDelay)
	RequestRestart()
	return nil
}

// RunPendingVacuum runs at startup, after the database is opened and before
// anything serves: it carries out a compaction requested by a cleanup, then
// makes its result the current cleanup job. It never stops the start: a
// failure is reported in the result. A marker left by a run that did not
// finish is reported as failed instead of being run again, so a VACUUM that
// crashes the process cannot restart it forever. The database path comes
// from SQLite, as when the compaction was requested, so a file: URI DSN
// finds its markers too.
func RunPendingVacuum() {
	if DB == nil {
		return
	}
	st, err := readDBFileStats(context.Background())
	if err != nil {
		log.Printf("NEZHA>> pending database compaction: %v", err)
		return
	}
	dbPath := st.path
	if dbPath == "" {
		return
	}
	pending := vacuumMarkerPath(dbPath, vacuumPendingFile)
	running := vacuumMarkerPath(dbPath, vacuumRunningFile)
	result := vacuumMarkerPath(dbPath, vacuumResultFile)

	if m, err := readVacuumMarker(running); err == nil || !errors.Is(err, os.ErrNotExist) {
		m.Job.State = model.HistoryCleanupFailed
		m.Job.Error = "the previous compaction did not finish: the dashboard stopped during VACUUM"
		finishVacuumOnStart(result, running, m)
	}
	if _, err := os.Stat(pending); err == nil {
		if err := os.Rename(pending, running); err != nil {
			log.Printf("NEZHA>> pending database compaction: %v", err)
			_ = os.Remove(pending)
		} else {
			finishVacuumOnStart(result, running, runVacuumOnStart(running))
		}
	}

	if m, err := readVacuumMarker(result); err == nil {
		setHistoryJob(m.Job)
	} else if !errors.Is(err, os.ErrNotExist) {
		log.Printf("NEZHA>> database compaction result: %v", err)
		_ = os.Remove(result)
	}
}

func runVacuumOnStart(running string) vacuumMarker {
	m, err := readVacuumMarker(running)
	if err != nil {
		m.Job.State = model.HistoryCleanupFailed
		m.Job.Error = fmt.Sprintf("the compaction request could not be read: %v", err)
		return m
	}
	ctx := context.Background()
	st, err := readDBFileStats(ctx)
	switch {
	case err != nil:
		m.Job.State = model.HistoryCleanupFailed
		m.Job.Error = err.Error()
	case !vacuumHasSpace(st):
		m.Job.State = model.HistoryCleanupSucceeded
		m.Job.VacuumSkipped = model.HistoryVacuumInsufficientDisk
	default:
		log.Println("NEZHA>> Compacting the database before the dashboard starts (VACUUM)...")
		started := time.Now()
		if err := execVacuum(ctx); err != nil {
			m.Job.State = model.HistoryCleanupFailed
			m.Job.Error = fmt.Sprintf("deleted %d rows, but VACUUM failed: %v", m.Job.DeletedRows, err)
		} else {
			m.Job.State = model.HistoryCleanupSucceeded
			log.Printf("NEZHA>> Database compacted in %s", time.Since(started).Round(time.Millisecond))
		}
	}
	if after, err := readDBFileStats(ctx); err == nil {
		m.Job.FreedSize = max(m.SizeBefore-after.size(), 0)
	}
	return m
}

// finishVacuumOnStart records m as the result and removes the running marker.
func finishVacuumOnStart(result, running string, m vacuumMarker) {
	m.Job.FinishedAt = formatJobTime(time.Now())
	if m.Job.State == model.HistoryCleanupFailed {
		log.Printf("NEZHA>> database compaction failed: %s", m.Job.Error)
	}
	if err := writeVacuumMarker(result, m); err != nil {
		log.Printf("NEZHA>> database compaction result could not be saved: %v", err)
		setHistoryJob(m.Job)
	}
	_ = os.Remove(running)
}
