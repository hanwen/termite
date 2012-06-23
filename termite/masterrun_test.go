package termite

import (
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/hanwen/termite/attr"
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
	beforeTime := tc.master.attributes.Get(rootless + "/dir").ChangeTime()
	var after *attr.FileAttr
	for i := 0; ; i++ {
		time.Sleep(10e6)
		subdir := fmt.Sprintf(tc.wd+"/dir/subdir%d", i)
		tc.RunSuccess(WorkRequest{
			Argv: []string{"mkdir", "-p", subdir},
		})
		after = tc.master.attributes.Get(strings.TrimLeft(subdir, "/"))
		if !after.ChangeTime().Equal(beforeTime) {
			break
		}
	}

	afterDir := tc.master.attributes.Get(rootless + "/dir")
	if afterDir.ChangeTime().Equal(beforeTime) {
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
	beforeTime := tc.master.attributes.Get(rootless + "/dir").ChangeTime()
	var after *attr.FileAttr
	for i := 0; ; i++ {
		time.Sleep(10e6)
		subdir := fmt.Sprintf(tc.wd+"/dir/subdir%d", i)
		tc.RunSuccess(WorkRequest{
			Argv: []string{"mkdir", subdir},
		})
		after = tc.master.attributes.Get(strings.TrimLeft(subdir, "/"))
		if !after.ChangeTime().Equal(beforeTime) {
			break
		}
	}

	afterDir := tc.master.attributes.Get(rootless + "/dir")
	if afterDir.ChangeTime().Equal(beforeTime) {
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
	if fi.Mode()&os.ModeType != 0 {
		t.Fatal("Should be regular file.")
	}

	fa := tc.master.attributes.Get(
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
	if fi, err := os.Lstat(tc.wd + "/a/b"); err != nil || !fi.IsDir() {
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

func TestEndToEndRmRfNoExist(t *testing.T) {
	tc := NewTestCase(t)
	defer tc.Clean()

	tc.refresh()

	tc.RunSuccess(WorkRequest{
		Argv: []string{"rm", "-rf", "does/not/exist"},
	})
	if fi, _ := os.Lstat(tc.wd + "/dir"); fi != nil {
		t.Fatalf("rm -r should remove everything: %v", fi)
	}
}

func TestEndToEndRmParentTimestamp(t *testing.T) {
	tc := NewTestCase(t)
	defer tc.Clean()

	err := os.MkdirAll(tc.wd+"/dir/subdir", 0755)
	check(err)
	rootless := strings.TrimLeft(tc.wd, "/")

	now := time.Now().Add(-10 * time.Second)
	err = os.Chtimes(tc.wd+"/dir", now, now)
	check(err)

	beforeTime := tc.master.attributes.Get(rootless + "/dir").ModTime()
	tc.RunSuccess(WorkRequest{
		Argv: []string{"rm", "-rf", tc.wd + "/dir/subdir"},
	})
	afterTime := tc.master.attributes.Get(rootless + "/dir").ModTime()
	if beforeTime.After(afterTime) {
		t.Errorf("Parent timestamp not changed after rm: before %d after %d",
			beforeTime, afterTime)
	}
}
