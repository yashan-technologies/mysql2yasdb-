package runtimedef

import (
	"os"
	"path"
	"path/filepath"

	"m2y/utils/stringutil"
)

const (
	_ENV_M2Y_HOME       = "M2Y_HOME"
	_ENV_M2Y_DEBUG_MODE = "M2Y_DEBUG_MODE"
)

const (
	_DIR_NAME_LOG    = "log"
	_DIR_NAME_CONFIG = "config"
	_DIR_NAME_EXPORT = "export"
)

var _m2yHome string

func GetM2YHome() string {
	return _m2yHome
}

func GetLogPath() string {
	return path.Join(_m2yHome, _DIR_NAME_LOG)
}

func GetExportPath() string {
	return path.Join(_m2yHome, _DIR_NAME_EXPORT)
}

func GetConfigPath() string {
	return path.Join(_m2yHome, _DIR_NAME_CONFIG)
}

func genHomeFromRelativePath() (home string, err error) {
	executeable, err := getExecutable()
	if err != nil {
		return
	}
	home, err = filepath.Abs(path.Dir(path.Dir(executeable)))
	return
}

func setM2YHome(v string) {
	_m2yHome = v
}

func initM2YHome() (err error) {
	m2yHome, err := genM2YHomeFromEnv()
	if err != nil {
		return
	}
	if !stringutil.IsEmpty(m2yHome) {
		setM2YHome(m2yHome)
		return
	}
	home, err := genHomeFromRelativePath()
	if err != nil {
		return
	}
	setM2YHome(home)
	return
}

func isDebugMode() bool {
	return !stringutil.IsEmpty(os.Getenv(_ENV_M2Y_DEBUG_MODE))
}

func getM2YHomeEnv() string {
	return os.Getenv(_ENV_M2Y_HOME)
}

func genM2YHomeFromEnv() (m2yHome string, err error) {
	m2yHomeEnv := getM2YHomeEnv()
	if isDebugMode() && !stringutil.IsEmpty(m2yHomeEnv) {
		m2yHomeEnv, err = filepath.Abs(m2yHomeEnv)
		if err != nil {
			return
		}
		m2yHome = m2yHomeEnv
		return
	}
	return
}
