package attr

import (
	"crypto"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TODO - move into fuse
func SplitPath(name string) (dir, base string) {
	dir, base = filepath.Split(name)
	dir = strings.TrimRight(dir, "/")
	return dir, base
}

func EncodeFileInfo(fi os.FileInfo) string {
	fi.Atime_ns = 0
	fi.Ino = 0
	fi.Dev = 0
	fi.Name = ""
	return fmt.Sprintf("%v", fi)
}

// for tests:
func TestStat(t *testing.T, n string) *os.FileInfo {
	t.Logf("test stat %q", n)
	f, _ := os.Lstat(n)
	return f
}

func TestGetattr(t *testing.T, n string) *FileAttr {
	t.Logf("test getattr %q", n)
	fi, _ := os.Lstat(n)
	a := FileAttr{
		FileInfo: fi,
	}
	if !a.Deletion() {
		a.ReadFromFs(n, crypto.MD5)
	}
	return &a
}
