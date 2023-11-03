package fileutil

import "path/filepath"

// GetRealPath returns the real path directly when path is a real path,
// and returns the real path pointed to by a link when path is a link.
func GetRealPath(path string) (realPath string, err error) {
	return filepath.EvalSymlinks(path)
}
