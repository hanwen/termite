package termite

import (
	"github.com/hanwen/go-fuse/fuse"
	"log"
	"io/ioutil"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

var _ = log.Printf

func testStat(t *testing.T, n string) *os.FileInfo {
	t.Logf("stat %q", n)
	f, _ := os.Lstat(n)
	return f
}

func getattr(t *testing.T, n string) *FileAttr {
	t.Logf("getattr %q", n)
	fi, _ := os.Lstat(n)
	a := FileAttr{
		FileInfo: fi,
	}
	if !a.Deletion() {
		a.ReadFromFs(n)
	}
	return &a
}

func TestAttrCacheNil(t *testing.T) {
	ac := NewAttributeCache(
		func(n string) *FileAttr {
			return nil
		},
		func(n string) *os.FileInfo {
			return nil
		})

	r := ac.Get("")
	if r == nil || !r.Deletion() {
		t.Errorf("should return deletion for error, got: %v", r)
	}
}

func TestAttrCache(t *testing.T) {
	dir, err := ioutil.TempDir("", "termite")
	check(err)
	syscall.Umask(0)

	ac := NewAttributeCache(
		func(n string) *FileAttr {
			return getattr(t, filepath.Join(dir, n))
		},
		func(n string) *os.FileInfo {
			return testStat(t, filepath.Join(dir, n))
		})
	err = ioutil.WriteFile(dir+"/file", []byte{42}, 0604)
	check(err)

	f := ac.Get("file")
	if f.Deletion() {
		t.Fatalf("Got deletion %v", f)
	}
	if f.Mode&0777 != 0604 {
		t.Fatalf("Got %o want %o", f.Mode&0777, 0604)
	}
	if !ac.Have("") {
		t.Fatalf("Must have parent too")
	}
	d := ac.GetDir("")
	if d.NameModeMap == nil || d.NameModeMap["file"] == 0 {
		t.Fatalf("root NameModeMap wrong %v", d.NameModeMap)
	}

	upd := FileAttr{
		Path:     "unknown/file",
		FileInfo: &os.FileInfo{Mode: fuse.S_IFLNK | 0666},
		Link:     "target",
	}

	ac.Update([]*FileAttr{&upd})
	if ac.Have("unknown/file") || ac.Have("unknown") {
		t.Fatalf("Should have ignored unknown directory")
	}

	// Make sure timestamps change.
	time.Sleep(150e6)
	err = ioutil.WriteFile(dir+"/other", []byte{43}, 0666)
	check(err)
	err = os.Chmod(dir+"/file", 0666)
	check(err)

	ac.Refresh("")

	d = ac.GetDir("")
	if d.NameModeMap["other"] == 0 {
		t.Fatalf("Should have 'other' in root %v", d)
	}
	f = ac.Get("file")
	if f.Mode&0777 != 0666 {
		t.Fatalf("Got %o , want 0666", f.Mode)
	}

}
