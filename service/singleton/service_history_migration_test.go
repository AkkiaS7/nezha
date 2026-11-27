package singleton

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/nezhahq/nezha/model"
)

// legacyServiceHistory is ServiceHistory as earlier releases declared it,
// with the single-column created_at index.
type legacyServiceHistory struct {
	ID        uint64    `gorm:"primaryKey"`
	CreatedAt time.Time `gorm:"index;<-:create;index:idx_server_id_created_at_service_id_avg_delay"`
	UpdatedAt time.Time `gorm:"autoUpdateTime"`
	ServiceID uint64    `gorm:"index:idx_server_id_created_at_service_id_avg_delay"`
	ServerID  uint64    `gorm:"index:idx_server_id_created_at_service_id_avg_delay"`
	AvgDelay  float64   `gorm:"index:idx_server_id_created_at_service_id_avg_delay"`
	Up        uint64
	Down      uint64
	Data      string
}

func (legacyServiceHistory) TableName() string { return "service_histories" }

const compositeHistoryIndex = "idx_server_id_created_at_service_id_avg_delay"

func TestMigrateServiceHistoryDropsLegacyCreatedAtIndex(t *testing.T) {
	newHistoryCleanupTestDB(t)
	require.NoError(t, DB.Migrator().DropTable(&model.ServiceHistory{}))
	require.NoError(t, DB.AutoMigrate(&legacyServiceHistory{}))
	require.True(t, DB.Migrator().HasIndex(&model.ServiceHistory{}, model.LegacyServiceHistoryCreatedAtIndex))
	require.NoError(t, DB.Create(&model.ServiceHistory{ServiceID: 1, ServerID: 1, CreatedAt: time.Now()}).Error)

	for range 2 { // the second run finds nothing to do
		require.NoError(t, migrateServiceHistory())
		require.False(t, DB.Migrator().HasIndex(&model.ServiceHistory{}, model.LegacyServiceHistoryCreatedAtIndex))
		require.True(t, DB.Migrator().HasIndex(&model.ServiceHistory{}, compositeHistoryIndex))
	}
	var n int64
	require.NoError(t, DB.Model(&model.ServiceHistory{}).Count(&n).Error)
	require.Equal(t, int64(1), n)
}

func TestNewServiceHistoryTableHasNoLegacyIndex(t *testing.T) {
	newHistoryCleanupTestDB(t)
	require.NoError(t, migrateServiceHistory())
	require.False(t, DB.Migrator().HasIndex(&model.ServiceHistory{}, model.LegacyServiceHistoryCreatedAtIndex))
	require.True(t, DB.Migrator().HasIndex(&model.ServiceHistory{}, compositeHistoryIndex))
}

// Every created_at range query on service history must still search the
// composite index after the single-column index is gone, not scan the table.
func TestServiceHistoryQueriesUseCompositeIndex(t *testing.T) {
	newHistoryCleanupTestDB(t)
	require.NoError(t, migrateServiceHistory())
	now := time.Now()
	for _, q := range []struct {
		sql  string
		args []any
	}{
		{"SELECT * FROM service_histories WHERE service_id = ? AND server_id != 0 AND created_at >= ? ORDER BY server_id, created_at", []any{1, now}},
		{"SELECT * FROM service_histories WHERE server_id = ? AND created_at >= ? ORDER BY service_id, created_at", []any{1, now}},
		{"SELECT * FROM service_histories WHERE created_at > ? AND created_at < ? AND server_id = 0", []any{now, now}},
		{"SELECT * FROM service_histories WHERE created_at >= ? AND server_id = 0", []any{now}},
		{"SELECT id FROM service_histories WHERE server_id != 0 AND created_at < ? LIMIT 5000", []any{now}},
		{"SELECT id FROM service_histories WHERE created_at >= ? ORDER BY created_at LIMIT 1", []any{now}},
	} {
		rows, err := DB.Raw("EXPLAIN QUERY PLAN "+q.sql, q.args...).Rows()
		require.NoError(t, err)
		var plan []string
		for rows.Next() {
			var id, parent, unused int
			var detail string
			require.NoError(t, rows.Scan(&id, &parent, &unused, &detail))
			plan = append(plan, detail)
		}
		require.NoError(t, rows.Close())
		require.Contains(t, strings.Join(plan, " | "), "INDEX "+compositeHistoryIndex, q.sql)
	}
}
