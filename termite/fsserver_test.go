package termite

// This file tests both rpcfs and fsserver by having them talk over a
// socketpair.

import (
	"github.com/hanwen/go-fuse/fuse"
	"io"
	"io/ioutil"
	"os"
	"rpc"
	"time"
	"testing"
)

// TODO - fold common code.

func init() {
	paranoia = true
}

func TestFsServerCache(t *testing.T) {
	me := newRpcFsTestCase(t)
	defer me.Clean()

	content := "hello"
	err := ioutil.WriteFile(me.orig+"/file.txt", []byte(content), 0644)
	if err != nil {
		t.Fatal(err)
	}

	me.server.refreshAttributeCache("")
	c := me.server.copyCache().Files
	if len(c) > 0 {
		t.Errorf("cache not empty? %#v", c)
	}

	os.Lstat(me.mnt + "/file.txt")
	c = me.server.copyCache().Files
	if len(c) != 2 {
		t.Errorf("cache should have 2 entries, got %#v", c)
	}
	name := "file.txt"
	ok := me.server.attr.Have(name)
	if !ok {
		t.Errorf("no entry for %q", name)
	}

	newName := me.orig + "/new.txt"
	err = os.Rename(me.orig + "/file.txt", newName)
	if err != nil {
		t.Fatal(err)
	}

	me.server.refreshAttributeCache("")
	ok = me.server.attr.Have(name)
	if ok {
		t.Errorf("after rename: entry for %q unexpected", name)
	}
}

type rpcFsTestCase struct {
	tmp  string
	mnt  string
	orig string

	cache  *ContentCache
	server *FsServer
	rpcFs  *RpcFs
	state  *fuse.MountState

	sockL, sockR io.ReadWriteCloser
}

func newRpcFsTestCase(t *testing.T) (me *rpcFsTestCase) {
	me = &rpcFsTestCase{}
	me.tmp, _ = ioutil.TempDir("", "term-fss")

	me.mnt = me.tmp + "/mnt"
	me.orig = me.tmp + "/orig"
	srvCache := me.tmp + "/server-cache"
	clientCache := me.tmp + "/client-cache"

	os.Mkdir(me.mnt, 0700)
	os.Mkdir(me.orig, 0700)

	cache := NewContentCache(srvCache)
	me.server = NewFsServer(me.orig, cache, []string{})
	me.server.excludePrivate = false

	var err os.Error
	me.sockL, me.sockR, err = fuse.Socketpair("unix")
	if err != nil {
		t.Fatal(err)
	}

	rpcServer := rpc.NewServer()
	rpcServer.Register(me.server)
	go rpcServer.ServeConn(me.sockL)

	rpcClient := rpc.NewClient(me.sockR)
	me.rpcFs = NewRpcFs(rpcClient, NewContentCache(clientCache))

	me.state, _, err = fuse.MountPathFileSystem(me.mnt, me.rpcFs, nil)
	me.state.Debug = fuse.VerboseTest()
	if err != nil {
		t.Fatal("Mount", err)
	}

	go me.state.Loop()
	return me
}

func (me *rpcFsTestCase) Clean() {
	err := me.state.Unmount()
	if err == nil {
		os.RemoveAll(me.tmp)
	} else {
		panic("fuse unmount failed.")
	}
	me.sockL.Close()
	me.sockR.Close()
}

func check(err os.Error) {
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
		if v.Name == "file.txt" {
			seen = true
		}
	}

	if !seen {
		t.Fatalf("Missing entry %q %v", "file.txt", entries)
	}

	time.Sleep(5e6)
	err = ioutil.WriteFile(me.orig + "/subdir/unstatted.txt", []byte("somethingelse"), 0644)
	check(err)
	err = os.Remove(me.orig + "/subdir/file.txt")
	check(err)

	fset := me.server.refreshAttributeCache("")
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

func TestRpcFS(t *testing.T) {
	me := newRpcFsTestCase(t)
	defer me.Clean()

	os.Mkdir(me.orig+"/subdir", 0700)
	content := "hello"
	err := ioutil.WriteFile(me.orig+"/file.txt", []byte(content), 0644)
	if err != nil {
		t.Fatal(err)
	}

	fi, err := os.Lstat(me.mnt + "/subdir")
	if fi == nil || !fi.IsDirectory() {
		t.Fatal("subdir stat", fi, err)
	}

	c, err := ioutil.ReadFile(me.mnt + "/file.txt")
	if err != nil || string(c) != "hello" {
		t.Error("Readfile", c)
	}

	entries, err := ioutil.ReadDir(me.mnt)
	if err != nil || len(entries) != 2 {
		t.Error("Readdir", err, entries)
	}

	// This test implementation detail - should be separate?
	a := me.server.attr.Get("file.txt")
	if a == nil || a.Hash == "" || string(a.Hash) != string(md5str(content)) {
		t.Errorf("cache error %v (%x)", a)
	}

	newcontent := "somethingelse"
	err = ioutil.WriteFile(me.orig+"/file.txt", []byte(newcontent), 0644)
	check(err)
	err = ioutil.WriteFile(me.orig+"/foobar.txt", []byte("more content"), 0644)
	check(err)

	me.server.refreshAttributeCache("")
	a = me.server.attr.Get("file.txt")
	if a == nil || a.Hash == "" || a.Hash != md5str(newcontent) {
		t.Errorf("refreshAttributeCache: cache error got %v, want %x", a, md5str(newcontent))
	}
}
