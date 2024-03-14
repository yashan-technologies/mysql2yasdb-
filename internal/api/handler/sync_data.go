package handler

import (
	"m2y/db"
	"m2y/defs/confdef"
	"m2y/internal/modules"
)

type SyncDataHandler struct {
	parallel      int
	tableParallel int
	batchSize     int
}

func NewSyncDataHandler(parallel, tableParallel, batchSize int) *SyncDataHandler {
	return &SyncDataHandler{parallel: parallel, tableParallel: tableParallel, batchSize: batchSize}
}

func (c *SyncDataHandler) SyncData() error {
	conf := confdef.GetM2YConfig()
	if len(conf.Mysql.Tables) != 0 {
		return modules.DealTableData(db.MysqlDB, db.YashanDB, conf.Mysql.Database, conf.Yashan.RemapSchemas[0], conf.Mysql.Tables, c.parallel, c.tableParallel, c.batchSize)
	}
	return modules.DealSchemasData(db.MysqlDB, db.YashanDB, conf.Mysql.Schemas, conf.Yashan.RemapSchemas, conf.Mysql.ExcludeTables, c.parallel, c.tableParallel, c.batchSize)
}
