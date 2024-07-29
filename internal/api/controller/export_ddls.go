package controller

import (
	"m2y/db"
	"m2y/defs/confdef"
	"m2y/internal/api/handler"
)

type M2YExportDDLsCmd struct{}

func (c *M2YExportDDLsCmd) Run() error {
	if err := c.validate(); err != nil {
		return err
	}
	if err := c.initDB(); err != nil {
		return err
	}
	return handler.NewExportDDLsHandler().ExportDDLs()
}

func (c *M2YExportDDLsCmd) validate() error {
	return nil
}

func (c *M2YExportDDLsCmd) initDB() error {
	if err := db.LoadMySQLDB(confdef.GetM2YConfig().MySQL); err != nil {
		return err
	}
	return nil
}
