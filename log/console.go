package log

import "go.uber.org/zap"

var (
	_consoleLogger *zap.Logger
	ConsoleSugar   *zap.SugaredLogger
)

func LoadConsoleLogger() {
	_consoleLogger = getLogger(_console_name, true)
	ConsoleSugar = _consoleLogger.Sugar()
}
