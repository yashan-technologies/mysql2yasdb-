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
	if len(config.MySQL.Tables) != 0 {
		return modules.DealTablesDDLs(db.MySQLDB, config.MySQL.Database, config.Yashan.RemapSchemas[0], config.MySQL.Schemas, false)
	}
	return modules.DealSchemasDDL(db.MySQLDB, config.MySQL.Schemas, config.Yashan.RemapSchemas, config.MySQL.ExcludeTables)
}
