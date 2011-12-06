package attr

import (
	"crypto"
	"fmt"
	"github.com/hanwen/go-fuse/fuse"	
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

func EncodeFileInfo(fi fuse.Attr) string {
	fi.Atime = 0
	fi.Atimensec = 0
	fi.Ino = 0
	fi.Rdev = 0
	return fmt.Sprintf("%v", fi)
}

// for tests:
func TestStat(t *testing.T, n string) *fuse.Attr {
	t.Logf("test stat %q", n)
	f, _ := os.Lstat(n)
	if f == nil {
		return nil
	}
	a := fuse.Attr{}
	a.FromFileInfo(f)
	return &a
}

func TestGetattr(t *testing.T, n string) (*FileAttr) {
	t.Logf("test getattr %q", n)
	fi, _ := os.Lstat(n)
	
	var fa *fuse.Attr
	if fi != nil {
		fa = &fuse.Attr{}
		fa.FromFileInfo(fi)
	}
	a := FileAttr{
		Attr: fa,
	}
	if !a.Deletion() {
		a.ReadFromFs(n, crypto.MD5)
	}
	return &a
}
