package controller

import (
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"

	"github.com/nezhahq/nezha/pkg/backup"
	"github.com/nezhahq/nezha/pkg/i18n"
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

// The database maintenance errors are in the catalogue: English is the
// msgid, and the Chinese catalogues translate them.
func TestDatabaseErrorsAreLocalized(t *testing.T) {
	old := singleton.Localizer
	t.Cleanup(func() { singleton.Localizer = old })

	singleton.Localizer = i18n.NewLocalizer("en_US", "nezha", "translations", i18n.Translations)
	require.Equal(t, "retention days must be between 1 and 3650", localizeDatabaseError(singleton.ErrInvalidRetentionDays).Error())
	require.Equal(t, singleton.ErrServiceHistoryUnavailable.Error(), localizeDatabaseError(singleton.ErrServiceHistoryUnavailable).Error())
	require.Equal(t, singleton.ErrDatabaseBusy.Error(), localizeDatabaseError(singleton.ErrDatabaseBusy).Error())
	require.Equal(t, backup.ErrDatabaseBusy.Error(), localizeBackupError(backup.ErrDatabaseBusy).Error())

	for lang, want := range map[string][]string{
		"zh_CN": {"保留天数必须在 1 到 3650 之间", "服务监控历史未存储在 SQLite 数据库中", "另一个数据库清理或维护正在进行"},
		"zh_TW": {"保留天數必須介於 1 到 3650 之間", "服務監控歷史未儲存在 SQLite 資料庫中", "另一個資料庫清理或維護作業正在進行"},
	} {
		singleton.Localizer = i18n.NewLocalizer(lang, "nezha", "translations", i18n.Translations)
		require.Equal(t, want[0], localizeDatabaseError(singleton.ErrInvalidRetentionDays).Error(), lang)
		require.Equal(t, want[1], localizeDatabaseError(singleton.ErrServiceHistoryUnavailable).Error(), lang)
		require.Equal(t, want[2], localizeDatabaseError(singleton.ErrDatabaseBusy).Error(), lang)
		require.Equal(t, want[2], localizeBackupError(backup.ErrDatabaseBusy).Error(), lang)
	}
}
