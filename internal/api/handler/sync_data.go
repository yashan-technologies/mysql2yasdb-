package handler

import (
	"m2y/db"
	"m2y/defs/confdef"
	"m2y/internal/modules"
	"m2y/log"
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
	log.Logger.Infof("parallel: %d\ttableParallel: %d\tbatchSize: %d\t", c.parallel, c.tableParallel, c.batchSize)
	conf := confdef.GetM2YConfig()
	if len(conf.Mysql.Tables) != 0 {
		return modules.DealTableData(db.MysqlDB, db.YashanDB, conf.Mysql.Database, conf.Yashan.RemapSchemas[0], conf.Mysql.Tables, c.parallel, c.tableParallel, c.batchSize)
	}
	return modules.DealSchemasData(db.MysqlDB, db.YashanDB, conf.Mysql.Schemas, conf.Yashan.RemapSchemas, conf.Mysql.ExcludeTables, c.parallel, c.tableParallel, c.batchSize)
}
