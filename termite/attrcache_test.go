package termite

import (
	"fmt"
	"github.com/hanwen/go-fuse/fuse"
	"log"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

var _ = log.Printf

func testStat(t *testing.T, n string) *os.FileInfo {
	t.Logf("test stat %q", n)
	f, _ := os.Lstat(n)
	return f
}

func testGetattr(t *testing.T, n string) *FileAttr {
	t.Logf("test getattr %q", n)
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

func attrCacheTestCase(t *testing.T) (*AttributeCache, string, func()) {
	dir, err := ioutil.TempDir("", "termite")
	check(err)
	syscall.Umask(0)

	ac := NewAttributeCache(
		func(n string) *FileAttr {
			return testGetattr(t, filepath.Join(dir, n))
		},
		func(n string) *os.FileInfo {
			return testStat(t, filepath.Join(dir, n))
		})
	return ac, dir, func() {
		os.RemoveAll(dir)
	}
}

func TestAttrCache(t *testing.T) {
	ac, dir, clean := attrCacheTestCase(t)
	defer clean()

	err := ioutil.WriteFile(dir+"/file", []byte{42}, 0604)
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

	fi, _ := os.Lstat(dir + "/file")

	// Make sure timestamps change.
	for {
		newFi, _ := os.Lstat(dir + "/file")
		if newFi.Ctime_ns != fi.Ctime_ns {
			break
		}
		time.Sleep(15e6)
		err = os.Chmod(dir+"/file", 0666)
		check(err)
	}

	err = ioutil.WriteFile(dir+"/other", []byte{43}, 0666)
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

func TestAttrCacheRefresh(t *testing.T) {
	ac, dir, clean := attrCacheTestCase(t)
	defer clean()

	os.Mkdir(dir+"/a", 0755)
	d := ac.GetDir("")
	if len(d.NameModeMap) != 1 || d.NameModeMap["a"] == 0 {
		t.Fatal("GetDir fail.", d.NameModeMap)
	}

	os.Remove(dir + "/a")

	i := 0
	for {
		newFi, err := os.Lstat(dir)
		check(err)
		if newFi.Ctime_ns != d.Ctime_ns {
			break
		}
		err = os.Mkdir(dir+fmt.Sprintf("/d%d", i), 0755)
		check(err)

		time.Sleep(10e6)
		i++
	}

	ac.Refresh("")

	d2 := ac.GetDir("")

	if d2.NameModeMap["a"] != 0 {
		t.Fatal("a should have disappeared.")
	}
}

type testClient struct {
	id    string
	attrs []*FileAttr
}

func (me *testClient) Id() string {
	return me.id
}

func (me *testClient) Send(attrs []*FileAttr) os.Error {
	for _, a := range attrs {
		me.attrs = append(me.attrs, a.Copy(true))
		if strings.Contains(a.Path, "delay") {
			time.Sleep(a.Size * 1e6)
		}
	}
	return nil
}

func TestAttrCacheClientBasic(t *testing.T) {
	ac, _, clean := attrCacheTestCase(t)
	defer clean()

	cl := testClient{
		id: "testid",
	}

	ac.AddClient(&cl)

	fa1 := FileAttr{
		Path:     "f1",
		FileInfo: &os.FileInfo{Mode: syscall.S_IFREG | 0644},
	}
	fs := FileSet{
		Files: []*FileAttr{&fa1},
	}
	ac.Queue(fs)

	err := ac.Send(&cl)
	check(err)

	if len(cl.attrs) != 1 {
		t.Errorf("Send error: %s", cl.attrs)
	}
}

func TestAttrCacheClientExtra(t *testing.T) {
	ac, dir, clean := attrCacheTestCase(t)
	defer clean()

	err := ioutil.WriteFile(dir+"/file.txt", []byte{42}, 0644)
	check(err)

	f := ac.Get("file.txt")
	if f.Deletion() {
		t.Fatalf("'file.txt' should be present.")
	}

	cl := testClient{
		id: "testid",
	}

	ac.AddClient(&cl)
	err = ac.Send(&cl)
	check(err)

	if len(cl.attrs) != 2 {
		t.Errorf("Send error: %s", cl.attrs)
	}
}

func TestAttrCacheClientWait(t *testing.T) {
	ac, _, clean := attrCacheTestCase(t)
	defer clean()

	cl := testClient{
		id: "testid",
	}

	ac.AddClient(&cl)
	d := int64(60)
	fa1 := FileAttr{
		Path: fmt.Sprintf("delay%d", d),
		FileInfo: &os.FileInfo{
			Mode: syscall.S_IFREG | 0644,
			Size: d,
		},
	}
	fs := FileSet{Files: []*FileAttr{&fa1}}
	ac.Queue(fs)
	done := make(chan int, 2)
	start := make(chan int, 1)
	func() {
		start <- 1
		err := ac.Send(&cl)
		check(err)
		done <- 1
	}()

	fs2 := FileSet{Files: []*FileAttr{}}
	<-start
	ac.Queue(fs2)

	time.Sleep(d / 2 * 1e6)
	err2 := ac.Send(&cl)
	check(err2)
	done <- 2

	if <-done != 1 {
		t.Errorf("Order incorrect. ")
	}
	<-done
}

type attrClient struct {
	id   string
	attr *AttributeCache
}

func (me *attrClient) Id() string {
	return me.id
}

func (me *attrClient) Send(attrs []*FileAttr) os.Error {
	me.attr.Update(attrs)
	return nil
}

func TestAttrCacheIncompleteDir(t *testing.T) {
	ac, _, clean := attrCacheTestCase(t)
	defer clean()
	cl := attrClient{
		id:   "testid",
		attr: NewAttributeCache(nil, nil),
	}

	ac.AddClient(&cl)

	root := FileAttr{
		FileInfo: &os.FileInfo{
			Mode: syscall.S_IFDIR | 0644,
		},
		NameModeMap: map[string]FileMode{
			"a": FileMode(syscall.S_IFDIR),
		},
		Path: "",
	}
	fs := FileSet{Files: []*FileAttr{&root}}
	ac.Queue(fs)
	ac.Send(&cl)

	// timestamp update.
	dir := FileAttr{
		FileInfo: &os.FileInfo{
			Mode:     syscall.S_IFDIR | 0755,
			Ctime_ns: 100,
		},
		Path: "a",
	}
	// entry update.
	child := FileAttr{
		FileInfo: &os.FileInfo{
			Mode: syscall.S_IFREG | 0644,
		},
		Path: "a/file.txt",
	}

	fs = FileSet{Files: []*FileAttr{&dir, &child}}
	ac.Queue(fs)
	ac.Send(&cl)

	g := cl.attr.localGet("a", false)
	if g != nil {
		t.Errorf("Client should ignore timestamp update to unknown directory: %v", g)
	}
}
