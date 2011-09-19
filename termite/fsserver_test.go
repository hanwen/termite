package termite

// This file tests both rpcfs and fsserver by having them talk over a
// socketpair.

import (
	"github.com/hanwen/go-fuse/fuse"
	"io"
	"io/ioutil"
	"log"
	"os"
	"rpc"
	"testing"
)

// TODO - fold common code.

func init() {
	paranoia = true
}


func TestFsServerCache(t *testing.T) {
	paranoia = true
	log.Println("TestFsServerCache")
	tmp, _ := ioutil.TempDir("", "term-fss")
	defer os.RemoveAll(tmp)

	orig := tmp + "/orig"
	srvCache := tmp + "/server-cache"

	err := os.Mkdir(orig, 0700)
	if err != nil {
		t.Fatal(err)
	}

	content := "hello"
	err = ioutil.WriteFile(orig+"/file.txt", []byte(content), 0644)
	if err != nil {
		t.Fatal(err)
	}

	cache := NewContentCache(srvCache)
	server := NewFsServer("/", cache, nil)
	server.excludePrivate = false

	server.refreshAttributeCache(orig)
	if len(server.attrCache) > 0 {
		t.Errorf("cache not empty? %#v", server.attrCache)
	}

	server.oneGetAttr(orig)
	server.oneGetAttr(orig+"/file.txt")

	if len(server.attrCache) != 2 {
		t.Errorf("cache should have 2 entries, got %#v", server.attrCache)
	}
	name := orig + "/file.txt"
	attr, ok := server.attrCache[name]
	if !ok || !attr.FileInfo.IsRegular() || attr.FileInfo.Size != int64(len(content)) {
		t.Errorf("entry for %q unexpected: %v %#v", name, ok, attr)
	}

	newName := orig + "/new.txt"
	err = os.Rename(name, newName)
	if err != nil {
		t.Fatal(err)
	}

	server.refreshAttributeCache(orig)
	attr, ok = server.attrCache[name]
	if !ok || attr.Status.Ok() {
		t.Errorf("after rename: entry for %q unexpected: %v %#v", name, ok, attr)
	}
}


type rpcFsTestCase struct {
	tmp string
	mnt string
	orig string

	cache *ContentCache
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
	me.state.Debug = true
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
	storedHash := me.server.hashCache["/file.txt"]
	if storedHash == "" || string(storedHash) != string(md5str(content)) {
		t.Errorf("cache error %x (%v)", storedHash, storedHash)
	}

	newData := []*FileAttr{
		&FileAttr{
			Path: "/file.txt",
			Hash: md5str("somethingelse"),
		},
		&FileAttr{
			Path: "/foobar.txt",
			Hash: md5str("contentsoffoobar"),
		},
	}
	me.server.updateFiles(newData)
	storedHash = me.server.hashCache["/file.txt"]
	if storedHash == "" || storedHash != newData[0].Hash {
		t.Errorf("cache error %x (%v)", storedHash, storedHash)
	}
}
