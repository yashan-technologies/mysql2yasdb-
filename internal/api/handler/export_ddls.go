package handler

import (
	"m2y/db"
	"m2y/defs/confdef"
	"m2y/internal/modules"
)

type ExportDDLsHandler struct{}

func NewExportDDLsHandler() *ExportDDLsHandler {
	return &ExportDDLsHandler{}
}

func (c *ExportDDLsHandler) ExportDDLs() error {
	config := confdef.GetM2YConfig()
	if len(config.Mysql.Tables) != 0 {
		return modules.DealTablesDDLs(db.MysqlDB, config.Mysql.Database, config.Yashan.RemapSchemas[0], config.Mysql.Schemas, false)
	}
	return modules.DealSchemasDDL(db.MysqlDB, config.Mysql.Schemas, config.Yashan.RemapSchemas, config.Mysql.ExcludeTables)
}
