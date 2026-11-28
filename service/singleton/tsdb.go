package singleton

import (
	"errors"
	"log"
	"time"

	"github.com/nezhahq/nezha/model"
	"github.com/nezhahq/nezha/pkg/tsdb"
)

var TSDBShared *tsdb.TSDB

var openTSDB = tsdb.Open

func InitTSDB() error {
	config := &tsdb.Config{
		RetentionDays:      30,
		MinFreeDiskSpaceGB: 1,
		MaxMemoryMB:        256,
	}

	if Conf.TSDB.DataPath != "" {
		config.DataPath = Conf.TSDB.DataPath
	}
	if Conf.TSDB.RetentionDays > 0 {
		config.RetentionDays = Conf.TSDB.RetentionDays
	}
	if Conf.TSDB.MinFreeDiskSpaceGB > 0 {
		config.MinFreeDiskSpaceGB = Conf.TSDB.MinFreeDiskSpaceGB
	}
	if Conf.TSDB.MaxMemoryMB > 0 {
		config.MaxMemoryMB = Conf.TSDB.MaxMemoryMB
	}
	if Conf.TSDB.WriteBufferSize > 0 {
		config.WriteBufferSize = Conf.TSDB.WriteBufferSize
	}
	if Conf.TSDB.WriteBufferFlushInterval > 0 {
		config.WriteBufferFlushInterval = time.Duration(Conf.TSDB.WriteBufferFlushInterval) * time.Second
	}

	if !config.Enabled() {
		log.Println("NEZHA>> TSDB is disabled (tsdb.data_path not configured)")
		if DB != nil {
			return migrateServiceHistory()
		}
		return nil
	}

	TSDBShared = nil
	db, err := openTSDB(config)
	if err != nil {
		if errors.Is(err, tsdb.ErrDiskFull) {
			log.Printf("NEZHA>> Warning: TSDB is unavailable because its disk is full; dashboard and alerts will continue without TSDB writes: %v", err)
			if DB != nil {
				if migrateErr := migrateServiceHistory(); migrateErr != nil {
					log.Printf("NEZHA>> Warning: failed to prepare SQLite service history fallback: %v", migrateErr)
				}
			}
			return nil
		}
		return err
	}
	TSDBShared = db

	log.Println("NEZHA>> TSDB initialized successfully")

	if DB != nil && DB.Migrator().HasTable("service_histories") {
		log.Println("NEZHA>> Dropping legacy service_histories table (TSDB is now enabled). Historical data will NOT be migrated.")
		if err := DB.Migrator().DropTable("service_histories"); err != nil {
			log.Printf("NEZHA>> Warning: failed to drop service_histories table: %v", err)
		}
	}

	return nil
}

// migrateServiceHistory prepares the SQLite service history table used when
// TSDB is off, and drops the single-column created_at index of earlier
// releases: the composite index starts with created_at and serves the same
// queries. Failing to drop it only costs space, so it is logged.
func migrateServiceHistory() error {
	if err := DB.AutoMigrate(model.ServiceHistory{}); err != nil {
		return err
	}
	if DB.Migrator().HasIndex(&model.ServiceHistory{}, model.LegacyServiceHistoryCreatedAtIndex) {
		if err := DB.Migrator().DropIndex(&model.ServiceHistory{}, model.LegacyServiceHistoryCreatedAtIndex); err != nil {
			log.Printf("NEZHA>> Warning: failed to drop index %s: %v", model.LegacyServiceHistoryCreatedAtIndex, err)
		}
	}
	return nil
}

func TSDBEnabled() bool {
	return TSDBShared != nil && !TSDBShared.IsClosed()
}

func CloseTSDB() {
	if TSDBShared != nil {
		TSDBShared.Close()
	}
}
