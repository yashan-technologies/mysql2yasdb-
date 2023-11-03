package log

import (
	"fmt"
	"m2y/commons"
	"m2y/define/runtimedef"
	"os"
	"path"
	"time"

	"git.yasdb.com/go/yasutil/fs"
	"github.com/natefinch/lumberjack"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const (
	_console_name = "console"
	_error_name   = "error"
)

const (
	_default_level = "DEBUG"
)

func formatTimeEncoder(t time.Time, enc zapcore.PrimitiveArrayEncoder) {
	enc.AppendString(t.Format(commons.TIME_FORMAT))
}

func getEncoderConfig() zapcore.EncoderConfig {
	cfg := zap.NewDevelopmentEncoderConfig()
	cfg.EncodeTime = formatTimeEncoder
	return cfg
}

func atomLevel(level string) zap.AtomicLevel {
	atomicLevel := zap.NewAtomicLevel()
	if err := atomicLevel.UnmarshalText([]byte(level)); err != nil {
		fmt.Printf("zap atom unmarshal err: %s", err.Error())
	}
	return atomicLevel
}
func getCore(servername string, console bool) zapcore.Core {
	hook := getFsHook(servername)
	syncers := []zapcore.WriteSyncer{zapcore.AddSync(&hook)}
	if console {
		// 打印到控制台
		syncers = append(syncers, zapcore.AddSync(os.Stdout))
	}
	return zapcore.NewCore(
		zapcore.NewConsoleEncoder(getEncoderConfig()), // 编码器配置
		zapcore.NewMultiWriteSyncer(syncers...),
		atomLevel(_default_level), // 日志级别
	)
}

func getLogger(servername string, console bool) *zap.Logger {
	//// 构造日志

	return zap.New(getCore(servername, console), zap.AddCaller(), zap.Development())
}

func createLogPath(logPath string) {
	if err := fs.Mkdir(logPath); err != nil {
		fmt.Printf("mkdir %s err: %s", logPath, err.Error())
	}
}

func getFsHook(servername string) lumberjack.Logger {
	logPath := path.Join(runtimedef.GetHome(), "log")
	createLogPath(logPath)
	logFile := path.Join(logPath, fmt.Sprintf("%s.log", servername))
	return lumberjack.Logger{
		Filename:   logFile, // 日志文件路径
		MaxSize:    10,      // 每个日志文件保存的最大尺寸 单位：M
		MaxBackups: 2,       // 日志文件最多保存多少个备份
		MaxAge:     7,       // 文件最多保存多少天
		Compress:   true,    // 是否压缩
	}
}

func InitLog() {
	LoadConsoleLogger()
	LoadErrorLogger()
}
