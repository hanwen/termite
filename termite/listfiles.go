package termite

import (
	"os"
	"path/filepath"
)

type fileLister map[string]os.FileInfo

func (me *fileLister) VisitFile(path string, f *os.FileInfo) {
	(*me)[path] = *f
}

func (me *fileLister) VisitDir(path string, f *os.FileInfo) bool {
	(*me)[path] = *f
	return true
}

func ListFilesRecursively(root string) map[string]os.FileInfo {
	l := fileLister{}
	filepath.Walk(root, &l, nil)
	return l
}
