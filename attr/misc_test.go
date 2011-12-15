package attr

import (
	"crypto"
	"github.com/hanwen/go-fuse/fuse"
	"os"
	"testing"
)

// for tests:
func StatForTest(t *testing.T, n string) *fuse.Attr {
	t.Logf("test stat %q", n)
	f, _ := os.Lstat(n)
	if f == nil {
		return nil
	}
	a := fuse.Attr{}
	a.FromFileInfo(f)
	return &a
}

func GetattrForTest(t *testing.T, n string) *FileAttr {
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
