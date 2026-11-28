package controller

import (
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"

	"github.com/nezhahq/nezha/service/singleton"
)

func TestDatabaseUsageRejectsInvalidRetention(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, q := range []string{"abc", "0", "-5", "3651", "1.5"} {
		c, _ := gin.CreateTestContext(httptest.NewRecorder())
		c.Request = httptest.NewRequest("GET", "/api/v1/database/usage?retention_days="+q, nil)
		// The check comes before any database access (DB may be nil here).
		u, err := getDatabaseUsage(c)
		require.Error(t, err, q)
		require.Nil(t, u, q)
		require.Equal(t, localizeDatabaseError(singleton.ErrInvalidRetentionDays).Error(), err.Error(), q)
	}
}

func TestHistoryCleanupRejectsInvalidBody(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, body := range []string{"", "{", `{"retention_days":"30"}`, `{"retention_days":0,"vacuum":true}`} {
		c, _ := gin.CreateTestContext(httptest.NewRecorder())
		c.Request = httptest.NewRequest("POST", "/api/v1/database/history-cleanup", strings.NewReader(body))
		c.Request.Header.Set("Content-Type", "application/json")
		_, err := startHistoryCleanup(c)
		require.Error(t, err, body)
	}
}
