package controller

import (
	"errors"
	"strconv"

	"github.com/gin-gonic/gin"

	"github.com/nezhahq/nezha/model"
	"github.com/nezhahq/nezha/service/singleton"
)

// Get database usage
// @Summary Get database usage
// @Security BearerAuth
// @Schemes
// @Description Size of the SQLite database, service history statistics and the state of the history cleanup. With retention_days, also estimates the rows a cleanup would delete and the space it would free.
// @Tags admin required
// @param retention_days query int false "Retention in days to estimate (1-3650)"
// @Produce json
// @Success 200 {object} model.CommonResponse[model.DatabaseUsage]
// @Router /database/usage [get]
func getDatabaseUsage(c *gin.Context) (*model.DatabaseUsage, error) {
	days := 0
	if s := c.Query("retention_days"); s != "" {
		d, err := strconv.Atoi(s)
		if err != nil || !singleton.ValidHistoryRetentionDays(d) {
			return nil, localizeDatabaseError(singleton.ErrInvalidRetentionDays)
		}
		days = d
	}
	u, err := singleton.GetDatabaseUsage(c.Request.Context(), days)
	if err != nil {
		return nil, newGormError("%v", err)
	}
	return u, nil
}

// Clean up service history
// @Summary Clean up service history
// @Security BearerAuth
// @Schemes
// @Description Starts deleting service history older than retention_days in the background (15-minute summaries are kept for at least 30 days), then compacts the database with VACUUM when vacuum is true. Poll GET /database/usage for the job state.
// @Tags admin required
// @Accept json
// @param request body model.HistoryCleanupRequest true "Cleanup request"
// @Produce json
// @Success 200 {object} model.CommonResponse[model.HistoryCleanupJob]
// @Router /database/history-cleanup [post]
func startHistoryCleanup(c *gin.Context) (*model.HistoryCleanupJob, error) {
	var req model.HistoryCleanupRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		return nil, err
	}
	job, err := singleton.StartHistoryCleanup(req.RetentionDays, req.Vacuum)
	if err != nil {
		return nil, localizeDatabaseError(err)
	}
	return &job, nil
}

// localizeDatabaseError returns the user-facing errors of the database
// maintenance endpoints through the gettext catalogue. Clients decide what to
// do from the response data (history_available, job.state), never from these
// texts.
func localizeDatabaseError(err error) error {
	l := singleton.Localizer
	if err == nil || l == nil {
		return err
	}
	switch {
	case errors.Is(err, singleton.ErrInvalidRetentionDays):
		return l.ErrorT("retention days must be between %d and %d", model.HistoryCleanupMinRetentionDays, model.HistoryCleanupMaxRetentionDays)
	case errors.Is(err, singleton.ErrServiceHistoryUnavailable):
		return l.ErrorT("service history is not stored in the SQLite database")
	case errors.Is(err, singleton.ErrDatabaseBusy):
		return l.ErrorT("another database cleanup or maintenance is in progress")
	}
	return err
}
