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
	HistoryCleanupSucceeded = "succeeded"
	HistoryCleanupFailed    = "failed"
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
	HistoryAvailable bool   `json:"history_available"`
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
}

// HistoryCleanupJob is the state of the last (or running) cleanup since the
// dashboard started.
type HistoryCleanupJob struct {
	State         string `json:"state"`
	RetentionDays int    `json:"retention_days"`
	Vacuum        bool   `json:"vacuum"`
	DeletedRows   int64  `json:"deleted_rows"`
	FreedSize     int64  `json:"freed_size"`
	VacuumSkipped string `json:"vacuum_skipped"`
	StartedAt     string `json:"started_at"`
	FinishedAt    string `json:"finished_at"`
	Error         string `json:"error"`
}

type HistoryCleanupRequest struct {
	RetentionDays int  `json:"retention_days"`
	Vacuum        bool `json:"vacuum"`
}
