package log

import "go.uber.org/zap"

var (
	_syncLogger *zap.Logger
	ErrorSugar  *zap.SugaredLogger
)

func LoadErrorLogger() {
	_syncLogger = getLogger(_error_name, false)
	ErrorSugar = _syncLogger.Sugar()
}
