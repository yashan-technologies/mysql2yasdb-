package controller

import (
	"m2y/db"
	"m2y/defs/confdef"
	"m2y/internal/api/handler"
)

type M2YCheckDataCmd struct {
	Parallel   int `name:"parallel"       short:"p" help:"Parallel number of check data."`
	SampleLine int `name:"sample-line"    short:"s" help:"Sample line of check data."`
}

func (c *M2YCheckDataCmd) Run() error {
	if err := c.validate(); err != nil {
		return err
	}
	if err := c.initDB(); err != nil {
		return err
	}
	return handler.NewCheckDataHandler(c.getCheckArgs()).CheckData()
}

func (c *M2YCheckDataCmd) validate() error {
	config := confdef.GetM2YConfig()
	if config.Mysql.SampleLines < 0 {
		return confdef.ErrSampleLines
	}
	return nil
}

func (c *M2YCheckDataCmd) initDB() error {
	if err := db.LoadMysqlDB(confdef.GetM2YConfig().Mysql); err != nil {
		return err
	}
	if err := db.LoadYashanDB(confdef.GetM2YConfig().Yashan); err != nil {
		return err
	}
	return nil
}

func (c *M2YCheckDataCmd) getCheckArgs() (parallel, sampleLine int) {
	parallel = getArgs(c.Parallel, confdef.GetM2YConfig().Mysql.Parallel, confdef.DefaultParallel, confdef.MaxParallel)
	sampleLine = getArgs(c.SampleLine, confdef.GetM2YConfig().Mysql.SampleLines, confdef.DefaultSampleLine, 0)
	return
}
