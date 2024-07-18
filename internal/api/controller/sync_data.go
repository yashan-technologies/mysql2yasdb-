package controller

import (
	"m2y/db"
	"m2y/defs/confdef"
	"m2y/internal/api/handler"
)

type M2YSyncDataCmd struct {
	Parallel      int `name:"parallel"       short:"p" help:"Parallel number of sync data."`
	BatchSize     int `name:"batch-size"     short:"b" help:"Batch size of sync data."`
	TableParallel int `name:"table-parallel" short:"t" help:"Parallel number of sync data per table."`
}

func (c *M2YSyncDataCmd) Run() error {
	if err := c.validate(); err != nil {
		return err
	}
	if err := c.initDB(); err != nil {
		return err
	}
	return handler.NewSyncDataHandler(c.getSyncArgs()).SyncData()
}

func (c *M2YSyncDataCmd) validate() error {
	if confdef.GetM2YConfig().Yashan.RemapSchemas == nil {
		return confdef.ErrNeedRemapSchemas
	}
	return nil
}

func (c *M2YSyncDataCmd) initDB() error {
	if err := db.LoadMySQLDB(confdef.GetM2YConfig().MySQL); err != nil {
		return err
	}
	if err := db.LoadYashanDB(confdef.GetM2YConfig().Yashan); err != nil {
		return err
	}
	return nil
}

func (c *M2YSyncDataCmd) getSyncArgs() (parallel, tableParallel, batchSize int) {
	conf := confdef.GetM2YConfig().MySQL
	parallel = getArgs(c.Parallel, conf.Parallel, confdef.DefaultParallel, confdef.MaxParallel)
	tableParallel = getArgs(c.TableParallel, conf.ParallelPerTable, confdef.DefaultParallelPerTable, confdef.MaxParallel)
	batchSize = getArgs(c.BatchSize, conf.BatchSize, confdef.DefaultBatchSize, 0)
	return
}

func getArgs(cmdArg, confArg, defaultArg, maxArg int) (res int) {
	if cmdArg > 0 {
		res = cmdArg
	} else if confArg > 0 {
		res = confArg
	} else {
		res = defaultArg
	}
	if res > maxArg && maxArg > 0 {
		res = maxArg
	}
	return res
}
