package termite

import (
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"testing"
	"time"
)

func TestEndToEndMkdirCleanPath(t *testing.T) {
	tc := NewTestCase(t)
	defer tc.Clean()

	tc.RunSuccess(WorkRequest{
		Argv: []string{"mkdir", "-p", tc.wd + "/a//b//c"},
	})
	tc.RunSuccess(WorkRequest{
		Argv: []string{"touch", "a//b//c/file.txt"},
	})
}

func TestEndToEndMkdirParentTimestamp(t *testing.T) {
	tc := NewTestCase(t)
	defer tc.Clean()

	tc.RunSuccess(WorkRequest{
		Argv: []string{"mkdir", "-p", tc.wd + "/dir"},
	})
	rootless := strings.TrimLeft(tc.wd, "/")
	beforeNs := tc.master.fileServer.attr.Get(rootless + "/dir").Ctime_ns
	var after *FileAttr
	for i := 0; ; i++ {
		time.Sleep(10e6)
		subdir := fmt.Sprintf(tc.wd+"/dir/subdir%d", i)
		tc.RunSuccess(WorkRequest{
			Argv: []string{"mkdir", "-p", subdir},
		})
		after = tc.master.fileServer.attr.Get(strings.TrimLeft(subdir, "/"))
		if after.Ctime_ns != beforeNs {
			break
		}
	}

	afterDir := tc.master.fileServer.attr.Get(rootless + "/dir")
	if afterDir.Ctime_ns == beforeNs {
		t.Errorf("Forgot to update parent timestamps")
	}
}

func TestEndToEndMkdirNoParentTimestamp(t *testing.T) {
	tc := NewTestCase(t)
	defer tc.Clean()

	tc.RunSuccess(WorkRequest{
		Argv: []string{"mkdir", "-p", tc.wd + "/dir"},
	})
	rootless := strings.TrimLeft(tc.wd, "/")
	beforeNs := tc.master.fileServer.attr.Get(rootless + "/dir").Ctime_ns
	var after *FileAttr
	for i := 0; ; i++ {
		time.Sleep(10e6)
		subdir := fmt.Sprintf(tc.wd+"/dir/subdir%d", i)
		tc.RunSuccess(WorkRequest{
			Argv: []string{"mkdir", subdir},
		})
		after = tc.master.fileServer.attr.Get(strings.TrimLeft(subdir, "/"))
		if after.Ctime_ns != beforeNs {
			break
		}
	}

	afterDir := tc.master.fileServer.attr.Get(rootless + "/dir")
	if afterDir.Ctime_ns == beforeNs {
		t.Errorf("Forgot to update parent timestamps")
	}
}

func TestEndToEndMkdirExist(t *testing.T) {
	tc := NewTestCase(t)
	defer tc.Clean()

	err := ioutil.WriteFile(tc.tmp+"/wd/file.txt", []byte{42}, 0644)
	check(err)

	tc.refresh()

	tc.RunFail(WorkRequest{
		Argv: []string{"mkdir", "file.txt"},
	})
	fi, _ := os.Lstat(tc.tmp + "/wd/file.txt")
	if !fi.IsRegular() {
		t.Fatal("Should be regular file.")
	}

	fa := tc.master.fileServer.attr.Get(
		strings.TrimLeft(tc.tmp+"/wd/file.txt", "/"))
	if fa == nil || fa.Deletion() || !fa.IsRegular() {
		t.Fatal("Attrcache out of sync", fa)
	}
}

func TestEndToEndMkdir(t *testing.T) {
	tc := NewTestCase(t)
	defer tc.Clean()

	err := ioutil.WriteFile(tc.tmp+"/wd/file.txt", []byte{42}, 0644)
	check(err)
	tc.refresh()

	tc.RunFail(WorkRequest{
		Argv: []string{"mkdir", "q/r"},
	})
	tc.RunFail(WorkRequest{
		Argv: []string{"mkdir", "file.txt/foo"},
	})
	tc.RunSuccess(WorkRequest{
		Argv: []string{"mkdir", "dir"},
	})
	tc.RunSuccess(WorkRequest{
		Argv: []string{"mkdir", "-p", "a/b"},
	})
	if fi, err := os.Lstat(tc.wd + "/a/b"); err != nil || !fi.IsDirectory() {
		t.Errorf("a/b should be a directory: Err %v, fi %v", err, fi)
	}
	tc.RunSuccess(WorkRequest{
		Argv: []string{"mkdir", "-p", "x/../y"},
	})

	fx, _ := os.Lstat(tc.wd + "/x")
	fy, _ := os.Lstat(tc.wd + "/y")
	if fx == nil || fy == nil {
		t.Errorf("mkdir x/../y should create both x and y: x=%v y=%v", fx, fy)
	}
}

func TestEndToEndRm(t *testing.T) {
	tc := NewTestCase(t)
	defer tc.Clean()

	err := ioutil.WriteFile(tc.wd+"/file.txt", []byte{42}, 0644)
	check(err)
	err = os.Mkdir(tc.wd+"/dir", 0755)
	check(err)
	tc.refresh()

	tc.RunFail(WorkRequest{
		Argv: []string{"rm", "noexist"},
	})

	tc.RunSuccess(WorkRequest{
		Argv: []string{"rm", "-f", "noexist"},
	})

	tc.RunFail(WorkRequest{
		Argv: []string{"rm", "dir"},
	})

	tc.RunFail(WorkRequest{
		Argv: []string{"rm", "-f", "dir"},
	})

	tc.RunSuccess(WorkRequest{
		Argv: []string{"rm", "file.txt"},
	})
	if fi, err := os.Lstat(tc.wd + "/file.txt"); err == nil || fi != nil {
		t.Errorf("should have been removed. Err %v, fi %v", err, fi)
	}
}

func TestEndToEndRmR(t *testing.T) {
	tc := NewTestCase(t)
	defer tc.Clean()

	os.Mkdir(tc.wd+"/dir", 0755)
	ioutil.WriteFile(tc.wd+"/dir/file.txt", []byte{42}, 0644)
	os.Mkdir(tc.wd+"/dir/subdir", 0755)
	ioutil.WriteFile(tc.wd+"/dir/subdir/file.txt", []byte{42}, 0644)
	tc.refresh()

	tc.RunSuccess(WorkRequest{
		Argv: []string{"rm", "-r", "dir"},
	})
	if fi, _ := os.Lstat(tc.wd + "/dir"); fi != nil {
		t.Fatalf("rm -r should remove everything: %v", fi)
	}
}
