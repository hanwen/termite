package termite

// This file tests both rpcfs and fsserver by having them talk over a
// socketpair.

import (
	"github.com/hanwen/go-fuse/fuse"
	"github.com/hanwen/termite/attr"
	"github.com/hanwen/termite/cba"
	"io"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestRpcFsFetchOnce(t *testing.T) {
	me := newRpcFsTestCase(t)
	defer me.Clean()
	ioutil.WriteFile(me.orig+"/file.txt", []byte{42}, 0644)
	me.attr.Refresh("")

	ioutil.ReadFile(me.mnt + "/file.txt")

	stats := me.server.stats.Timings()
	if stats == nil || stats["FsServer.FileContent"] == nil {
		t.Fatalf("Stats missing: %v", stats)
	}
	if stats["FsServer.FileContent"].N > 1 {
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
	ok := me.server.attributes.Have(name)
	if !ok {
		t.Errorf("no entry for %q", name)
	}

	newName := me.orig + "/new.txt"
	err = os.Rename(me.orig+"/file.txt", newName)
	if err != nil {
		t.Fatal(err)
	}

	me.attr.Refresh("")
	ok = me.server.attributes.Have(name)
	if ok {
		t.Errorf("after rename: entry for %q unexpected", name)
	}
}

type rpcFsTestCase struct {
	tmp  string
	mnt  string
	orig string

	cache  *cba.ContentCache
	attr   *attr.AttributeCache
	server *FsServer
	rpcFs  *RpcFs
	state  *fuse.MountState

	sockL, sockR io.ReadWriteCloser

	tester *testing.T
}

func (me *rpcFsTestCase) getattr(n string) *attr.FileAttr {
	p := filepath.Join(me.orig, n)
	a := attr.TestGetattr(me.tester, p)
	if a.Hash != "" {
		me.cache.SavePath(p)
	}
	return a
}

func newRpcFsTestCase(t *testing.T) (me *rpcFsTestCase) {
	me = &rpcFsTestCase{tester: t}
	me.tmp, _ = ioutil.TempDir("", "term-fss")

	me.mnt = me.tmp + "/mnt"
	me.orig = me.tmp + "/orig"
	srvCache := me.tmp + "/server-cache"
	clientCache := me.tmp + "/client-cache"

	os.Mkdir(me.mnt, 0700)
	os.Mkdir(me.orig, 0700)

	copts := cba.ContentCacheOptions{
		Dir: srvCache,
	}
	me.cache = cba.NewContentCache(&copts)
	me.attr = attr.NewAttributeCache(
		func(n string) *attr.FileAttr { return me.getattr(n) },
		func(n string) *fuse.Attr {
			return attr.TestStat(t, filepath.Join(me.orig, n))
		})
	me.attr.Paranoia = true
	me.server = NewFsServer(me.attr, me.cache)

	var err error
	me.sockL, me.sockR, err = unixSocketpair()
	if err != nil {
		t.Fatal(err)
	}

	rpcServer := rpc.NewServer()
	rpcServer.Register(me.server)
	go rpcServer.ServeConn(me.sockL)

	rpcClient := rpc.NewClient(me.sockR)
	cOpts := cba.ContentCacheOptions{
		Dir: clientCache,
	}
	cache := cba.NewContentCache(&cOpts)
	me.rpcFs = NewRpcFs(rpcClient, cache)
	me.rpcFs.id = "rpcfs_test"
	nfs := fuse.NewPathNodeFs(me.rpcFs, nil)
	me.state, _, err = fuse.MountNodeFileSystem(me.mnt, nfs, nil)
	me.state.Debug = fuse.VerboseTest()
	if err != nil {
		t.Fatal("Mount", err)
	}

	go me.state.Loop()
	return me
}

func (me *rpcFsTestCase) Clean() {
	if err := me.state.Unmount(); err != nil {
		log.Panic("fuse unmount failed.", err)
	}
	os.RemoveAll(me.tmp)
	me.sockL.Close()
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
	a := me.server.attributes.Get("file.txt")
	if a == nil || a.Hash == "" || string(a.Hash) != string(md5str(content)) {
		t.Errorf("cache error %v (%x)", a)
	}

	newcontent := "somethingelse"
	err = ioutil.WriteFile(me.orig+"/file.txt", []byte(newcontent), 0644)
	check(err)
	err = ioutil.WriteFile(me.orig+"/foobar.txt", []byte("more content"), 0644)
	check(err)

	me.attr.Refresh("")
	a = me.server.attributes.Get("file.txt")
	if a == nil || a.Hash == "" || a.Hash != md5str(newcontent) {
		t.Errorf("refreshAttributeCache: cache error got %v, want %x", a, md5str(newcontent))
	}
}
