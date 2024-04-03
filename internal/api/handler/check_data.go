package handler

import (
	"m2y/db"
	"m2y/defs/confdef"
	"m2y/internal/modules"
)

type CheckDataHandler struct {
	parallel   int
	sampleLine int
}

func NewCheckDataHandler(parallel, sampleLine int) *CheckDataHandler {
	return &CheckDataHandler{
		parallel:   parallel,
		sampleLine: sampleLine,
	}
}

func (c *CheckDataHandler) CheckData() error {
	conf := confdef.GetM2YConfig()
	var res [][]string
	var err error
	if len(conf.Mysql.Tables) != 0 {
		res, err = modules.CompareTables(db.MysqlDB, db.YashanDB, conf.Mysql.Database, conf.Yashan.RemapSchemas[0], conf.Mysql.Tables, c.parallel, c.sampleLine)
	} else {
		res, err = modules.CompareSchemas(db.MysqlDB, db.YashanDB, conf.Mysql.Schemas, conf.Yashan.RemapSchemas, conf.Mysql.ExcludeTables, c.parallel, c.sampleLine)
	}
	if err != nil {
		return err
	}
	modules.PrintCheckResults(res)
	return nil
}
