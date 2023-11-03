package runtimedef

import (
	"m2y/utils/fileutil"
	"os"
	"path"
	"path/filepath"
)

var _home string

func getExecutable() (executeable string, err error) {
	executeable, err = os.Executable()
	if err != nil {
		return
	}
	return fileutil.GetRealPath(executeable)
}

func genHomeFromRelativePath() (home string, err error) {
	executeable, err := getExecutable()
	if err != nil {
		return
	}
	home, err = filepath.Abs(path.Dir(executeable))
	return
}

func setHome(v string) {
	_home = v
}

func initHome() (err error) {
	home, err := genHomeFromRelativePath()
	if err != nil {
		return
	}
	setHome(home)
	return
}

func GetHome() string {
	return _home
}
