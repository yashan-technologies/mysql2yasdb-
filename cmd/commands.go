package main

import (
	"m2y/commons/flags"
	"m2y/internal/api/controller"
)

type App struct {
	flags.Globals
	SyncData   controller.M2YSyncDataCmd   `cmd:"sync"   name:"sync"   help:"Sync data from MySQL to YashanDB."`
	ExportDDLs controller.M2YExportDDLsCmd `cmd:"export" name:"export" help:"Export DDLs from MySQL."` // TODO: 暂时取名叫export;这个子命令名称有一些误导性，但是方便使用
	CheckData  controller.M2YCheckDataCmd  `cmd:"check"  name:"check"  help:"Check data from MySQL to YashanDB."`
}
