package model

// Retention bounds accepted by the service history cleanup, in days.
const (
	HistoryCleanupMinRetentionDays = 1
	HistoryCleanupMaxRetentionDays = 3650
	// HistorySummaryMinRetentionDays is how long the 15-minute summaries
	// (server_id = 0) are kept whatever retention is chosen: the 30-day
	// availability bars are rebuilt from them at startup.
	HistorySummaryMinRetentionDays = 30
)

// States of a service history cleanup job.
const (
	HistoryCleanupIdle      = "idle"
	HistoryCleanupDeleting  = "deleting"
	HistoryCleanupVacuuming = "vacuuming"
	// HistoryCleanupRestarting: the rows are deleted and the dashboard is
	// restarting to compact the database before it serves again.
	HistoryCleanupRestarting = "restarting"
	HistoryCleanupSucceeded  = "succeeded"
	HistoryCleanupFailed     = "failed"
)

// How a cleanup compacts the database.
const (
	HistoryVacuumModeNone   = "none"
	HistoryVacuumModeOnline = "online"
	// HistoryVacuumModeRestart compacts while the dashboard restarts, before
	// it opens HTTP and gRPC, so no request meets the locked database.
	HistoryVacuumModeRestart = "restart"
)

// Why a compaction mode is recommended.
const (
	// HistoryVacuumReasonShort: online compaction is expected to be short.
	HistoryVacuumReasonShort = "short"
	// HistoryVacuumReasonLong: online compaction is expected to outlast the
	// threshold, so compacting while restarting is recommended.
	HistoryVacuumReasonLong = "long"
	// HistoryVacuumReasonRestartUnavailable: online compaction is expected to
	// be long, but the dashboard cannot restart itself.
	HistoryVacuumReasonRestartUnavailable = "restart_unavailable"
)

// Reasons for a cleanup job not to compact the database.
const (
	HistoryVacuumNotRequested     = "not_requested"
	HistoryVacuumInsufficientDisk = "insufficient_disk"
)

// DatabaseUsage reports the size of the SQLite database and, for a given
// retention, how much the service history cleanup would remove. Sizes are in
// bytes, times are RFC 3339 strings ("" when unknown).
type DatabaseUsage struct {
	// DBSize is page_count * page_size, plus the -wal file if there is one.
	DBSize int64 `json:"db_size"`
	// FreePagesSize is the space VACUUM can give back right now.
	FreePagesSize int64 `json:"free_pages_size"`
	// DiskFree is the free space of the file system holding the database,
	// 0 when it cannot be determined.
	DiskFree int64 `json:"disk_free"`
	// HistoryAvailable is false when service history is not kept in SQLite
	// (TSDB enabled); the cleanup is unavailable then.
	HistoryAvailable bool `json:"history_available"`
	// RestartAvailable is true when the dashboard runs under systemd or in a
	// container and can restart itself (the supervisor's restart policy
	// itself cannot be seen); compacting while restarting needs it.
	RestartAvailable bool   `json:"restart_available"`
	HistoryRows      int64  `json:"history_rows"`
	HistoryOldestAt  string `json:"history_oldest_at"`
	// Estimate is present when retention_days was given and history is available.
	Estimate *HistoryCleanupEstimate `json:"estimate,omitempty"`
	Job      HistoryCleanupJob       `json:"job"`
}

type HistoryCleanupEstimate struct {
	RetentionDays int   `json:"retention_days"`
	DeletableRows int64 `json:"deletable_rows"`
	// EstimatedFreeSize is the expected shrink of the file after cleanup and VACUUM.
	EstimatedFreeSize int64 `json:"estimated_free_size"`
	// VacuumRequiredSize is the free disk space VACUUM needs after the cleanup.
	VacuumRequiredSize int64 `json:"vacuum_required_size"`
	// Approximate is true when the row count timed out and was approximated.
	Approximate bool `json:"approximate"`
	// SizeAfter is the expected database size after cleanup and VACUUM.
	SizeAfter int64 `json:"size_after"`
	// OnlineVacuumSeconds is the expected duration of an online VACUUM, in
	// which the dashboard does not answer (at least 1).
	OnlineVacuumSeconds int `json:"online_vacuum_seconds"`
	// RestartDowntimeSeconds is the expected time the dashboard is down when
	// compacting while restarting.
	RestartDowntimeSeconds int    `json:"restart_downtime_seconds"`
	RecommendedVacuumMode  string `json:"recommended_vacuum_mode"`
	RecommendationReason   string `json:"recommendation_reason"`
}

// HistoryCleanupJob is the state of the last (or running) cleanup since the
// dashboard started.
type HistoryCleanupJob struct {
	State         string `json:"state"`
	RetentionDays int    `json:"retention_days"`
	// Vacuum is VacuumMode != "none", kept for older clients.
	Vacuum        bool   `json:"vacuum"`
	VacuumMode    string `json:"vacuum_mode"`
	DeletedRows   int64  `json:"deleted_rows"`
	FreedSize     int64  `json:"freed_size"`
	VacuumSkipped string `json:"vacuum_skipped"`
	StartedAt     string `json:"started_at"`
	FinishedAt    string `json:"finished_at"`
	Error         string `json:"error"`
}

type HistoryCleanupRequest struct {
	RetentionDays int `json:"retention_days"`
	// VacuumMode is none, online or restart. Without it, Vacuum chooses
	// online (true) or none (false), as older clients send.
	VacuumMode string `json:"vacuum_mode,omitempty"`
	Vacuum     bool   `json:"vacuum,omitempty"`
}

// VacuumModeOrDefault returns the requested mode, or the one the older
// vacuum flag stands for.
func (r HistoryCleanupRequest) VacuumModeOrDefault() string {
	if r.VacuumMode != "" {
		return r.VacuumMode
	}
	if r.Vacuum {
		return HistoryVacuumModeOnline
	}
	return HistoryVacuumModeNone
}
