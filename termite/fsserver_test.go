package termite

// This file tests both rpcfs and fsserver by having them talk over a
// socketpair.

import (
	"crypto"
	"io"
	"io/ioutil"
	"log"
	"net/rpc"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/hanwen/go-fuse/fuse"
	"github.com/hanwen/go-fuse/fuse/nodefs"
	"github.com/hanwen/go-fuse/fuse/pathfs"
	"github.com/hanwen/termite/attr"
	"github.com/hanwen/termite/cba"
)

// UGH - copy & paste.
// for tests:
func StatForTest(t *testing.T, n string) *fuse.Attr {
	t.Logf("test stat %q", n)
	f, _ := os.Lstat(n)
	if f == nil {
		return nil
	}
	return fuse.ToAttr(f)
}

func GetattrForTest(t *testing.T, n string) *attr.FileAttr {
	t.Logf("test getattr %q", n)
	fi, _ := os.Lstat(n)

	var fa *fuse.Attr
	if fi != nil {
		fa = fuse.ToAttr(fi)
	}
	a := attr.FileAttr{
		Attr: fa,
	}
	if !a.Deletion() {
		a.ReadFromFs(n, crypto.MD5)
	}
	return &a
}

type rpcFsTestCase struct {
	tmp  string
	mnt  string
	orig string

	serverStore, clientStore *cba.Store
	attr                     *attr.AttributeCache
	server                   *attr.Server
	rpcFs                    *RpcFs
	state                    *fuse.Server

	sockL, sockR       io.ReadWriteCloser
	contentL, contentR io.ReadWriteCloser

	tester *testing.T
}

func (me *rpcFsTestCase) getattr(n string) *attr.FileAttr {
	p := filepath.Join(me.orig, n)
	a := GetattrForTest(me.tester, p)
	if a.Hash != "" {
		me.serverStore.SavePath(p)
	}
	return a
}

func netPair() (net.Conn, net.Conn, error) {
	l, err := net.Listen("tcp", ":0")
	if err != nil {
		return nil, nil, err
	}
	defer l.Close()
	c1, err := net.Dial("tcp", l.Addr().String())
	if err != nil {
		return nil, nil, err
	}

	c2, err := l.Accept()
	if err != nil {
		return nil, nil, err
	}
	
	return c1, c2, nil
}

func newRpcFsTestCase(t *testing.T) (me *rpcFsTestCase) {
	me = &rpcFsTestCase{tester: t}
	me.tmp, _ = ioutil.TempDir("", "term-fss")

	me.mnt = me.tmp + "/mnt"
	me.orig = me.tmp + "/orig"
	srvCache := me.tmp + "/server-cache"

	os.Mkdir(me.mnt, 0700)
	os.Mkdir(me.orig, 0700)

	copts := cba.StoreOptions{
		Dir: srvCache,
	}
	me.serverStore = cba.NewStore(&copts)
	me.attr = attr.NewAttributeCache(
		func(n string) *attr.FileAttr { return me.getattr(n) },
		func(n string) *fuse.Attr {
			return StatForTest(t, filepath.Join(me.orig, n))
		})
	me.attr.Paranoia = true
	me.server = attr.NewServer(me.attr)

	
	var err error
	me.sockL, me.sockR, err = netPair()
	if err != nil {
		t.Fatal(err)
	}
	me.contentL, me.contentR, err = netPair()
	if err != nil {
		t.Fatal(err)
	}

	rpcServer := rpc.NewServer()
	rpcServer.Register(me.server)
	go func() {
		rpcServer.ServeConn(me.sockL)
		me.sockL.Close()
	}()
	go func() {
		me.serverStore.ServeConn(me.contentL)
		me.contentL.Close()
	}()
	
	cOpts := cba.StoreOptions{
		Dir: me.tmp + "/client-cache",
	}
	me.clientStore = cba.NewStore(&cOpts)
	attrClient := attr.NewClient(me.sockR, "id")
	me.rpcFs = NewRpcFs(attrClient, me.clientStore, me.contentR)
	me.rpcFs.id = "rpcfs_test"
	nfs := pathfs.NewPathNodeFs(me.rpcFs, nil)
	me.state, _, err = nodefs.MountFileSystem(me.mnt, nfs, nil)
	me.state.SetDebug(fuse.VerboseTest())
	if err != nil {
		t.Fatal("Mount", err)
	}

	go me.state.Serve()
	return me
}

func (me *rpcFsTestCase) Clean() {
	if err := me.state.Unmount(); err != nil {
		log.Panic("fuse unmount failed.", err)
	}
	os.RemoveAll(me.tmp)
	me.contentR.Close()
	me.sockR.Close()
}

func check(err error) {
	if err != nil {
		panic(err)
	}
}

func TestRpcFsReadDirCache(t *testing.T) {
	me := newRpcFsTestCase(t)
	defer me.Clean()

	os.Mkdir(me.orig+"/subdir", 0700)
	content := "hello"
	err := ioutil.WriteFile(me.orig+"/subdir/file.txt", []byte(content), 0644)
	check(err)

	entries, err := ioutil.ReadDir(me.mnt + "/subdir")
	check(err)

	seen := false
	for _, v := range entries {
		if v.Name() == "file.txt" {
			seen = true
		}
	}

	if !seen {
		t.Fatalf("Missing entry %q %v", "file.txt", entries)
	}

	before, _ := os.Lstat(me.orig + "/subdir")
	for {
		err = ioutil.WriteFile(me.orig+"/subdir/unstatted.txt", []byte("somethingelse"), 0644)
		check(err)
		after, _ := os.Lstat(me.orig + "/subdir")
		if !before.ModTime().Equal(after.ModTime()) {
			break
		}
		time.Sleep(10e6)
		os.Remove(me.orig + "/subdir/unstatted.txt")
	}

	err = os.Remove(me.orig + "/subdir/file.txt")
	check(err)

	fset := me.attr.Refresh("")
	me.rpcFs.updateFiles(fset.Files)

	_, err = ioutil.ReadDir(me.mnt + "/subdir")
	check(err)

	dir := me.rpcFs.attr.GetDir("subdir")
	if dir == nil {
		t.Fatalf("Should have cache entry for /subdir")
	}

	if _, ok := dir.NameModeMap["file.txt"]; ok {
		t.Errorf("file.txt should have disappeared: %v", dir.NameModeMap)
	}
	if _, ok := dir.NameModeMap["unstatted.txt"]; !ok {
		t.Errorf("unstatted.txt should have appeared: %v", dir.NameModeMap)
	}
}

func TestRpcFsBasic(t *testing.T) {
	me := newRpcFsTestCase(t)
	defer me.Clean()

	os.Mkdir(me.orig+"/subdir", 0700)
	content := "hello"
	err := ioutil.WriteFile(me.orig+"/file.txt", []byte(content), 0644)
	if err != nil {
		t.Fatal(err)
	}

	fi, err := os.Lstat(me.mnt + "/subdir")
	if fi == nil || !fi.IsDir() {
		t.Fatal("subdir stat", fi, err)
	}

	c, err := ioutil.ReadFile(me.mnt + "/file.txt")
	if err != nil || string(c) != "hello" {
		t.Errorf("Readfile: want 'hello', got '%s', err %v", c, err)
	}

	entries, err := ioutil.ReadDir(me.mnt)
	if err != nil || len(entries) != 2 {
		t.Error("Readdir", err, entries)
	}

	// This test implementation detail - should be separate?
	a := me.attr.Get("file.txt")
	if a == nil || a.Hash == "" || string(a.Hash) != string(md5str(content)) {
		t.Errorf("cache error %v (%x)", a)
	}

	newcontent := "somethingelse"
	err = ioutil.WriteFile(me.orig+"/file.txt", []byte(newcontent), 0644)
	check(err)
	err = ioutil.WriteFile(me.orig+"/foobar.txt", []byte("more content"), 0644)
	check(err)

	me.attr.Refresh("")
	a = me.attr.Get("file.txt")
	if a == nil || a.Hash == "" || a.Hash != md5str(newcontent) {
		t.Errorf("refreshAttributeCache: cache error got %v, want %x", a, md5str(newcontent))
	}
}

func TestRpcFsFetchOnce(t *testing.T) {
	me := newRpcFsTestCase(t)
	defer me.Clean()
	ioutil.WriteFile(me.orig+"/file.txt", []byte{42}, 0644)
	me.attr.Refresh("")

	ioutil.ReadFile(me.mnt + "/file.txt")

	stats := me.serverStore.TimingMap()
	key := "ContentStore.Save"
	if stats == nil || stats[key] == nil {
		t.Fatalf("Stats %q missing: %v", key, stats)
	}
	if stats[key].N > 1 {
		t.Errorf("File content was served more than once.")
	}
}

func TestFsServerCache(t *testing.T) {
	me := newRpcFsTestCase(t)
	defer me.Clean()

	content := "hello"
	err := ioutil.WriteFile(me.orig+"/file.txt", []byte(content), 0644)
	me.attr.Refresh("")
	c := me.attr.Copy().Files
	if len(c) > 0 {
		t.Errorf("cache not empty? %#v", c)
	}

	os.Lstat(me.mnt + "/file.txt")
	c = me.attr.Copy().Files
	if len(c) != 2 {
		t.Errorf("cache should have 2 entries, got %#v", c)
	}
	name := "file.txt"
	ok := me.attr.Have(name)
	if !ok {
		t.Errorf("no entry for %q", name)
	}

	newName := me.orig + "/new.txt"
	err = os.Rename(me.orig+"/file.txt", newName)
	if err != nil {
		t.Fatal(err)
	}

	me.attr.Refresh("")
	ok = me.attr.Have(name)
	if ok {
		t.Errorf("after rename: entry for %q unexpected", name)
	}
}
