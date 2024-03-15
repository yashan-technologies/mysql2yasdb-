package main

import (
	"fmt"
	"strings"

	"m2y/commons/flags"
	"m2y/commons/std"
	"m2y/defs/compiledef"
	"m2y/defs/confdef"
	"m2y/defs/runtimedef"
	"m2y/log"

	"git.yasdb.com/go/yaserr"
	"github.com/alecthomas/kong"
)

const (
	_APP_NAME        = "mysql2yasdb"
	_APP_DESCRIPTION = "mysql2yasdb is a tool for synchronizing data from MySQL to YashanDB."
)

func main() {
	var app App
	options := flags.NewAppOptions(_APP_NAME, _APP_DESCRIPTION, compiledef.GetAPPVersion())
	ctx := kong.Parse(&app, options...)
	if err := initApp(app); err != nil {
		ctx.FatalIfErrorf(err)
	}
	finalize := std.GetRedirecter().RedirectStd()
	defer finalize()
	std.WriteToFile(fmt.Sprintf("execute: %s %s\n", _APP_NAME, strings.Join(ctx.Args, " ")))
	if err := ctx.Run(); err != nil {
		log.Logger.Error(yaserr.Unwrap(err))
	}
}

func initLogger(logPath, level string) error {
	optFuncs := []log.OptFunc{
		log.SetLogPath(logPath),
		log.SetLevel(level),
		log.SetConsole(true),
	}
	return log.InitLogger(_APP_NAME, log.NewLogOption(optFuncs...))
}

func initApp(app App) error {
	if err := runtimedef.InitRuntime(); err != nil {
		return err
	}
	if err := confdef.InitM2YConfig(app.Config); err != nil {
		return err
	}
	if err := initLogger(runtimedef.GetLogPath(), confdef.GetM2YConfig().LogLevel); err != nil {
		return err
	}
	if err := std.InitRedirecter(); err != nil {
		return err
	}
	return nil
}
