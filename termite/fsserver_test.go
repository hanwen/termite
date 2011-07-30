package termite

// This file tests both rpcfs and fsserver by having them talk over a
// socketpair.

import (
	"github.com/hanwen/go-fuse/fuse"
	"io/ioutil"
	"log"
	"os"
	"rpc"
	"testing"
)

func TestRpcFS(t *testing.T) {
	tmp, _ := ioutil.TempDir("", "")

	mnt := tmp + "/mnt"
	orig := tmp + "/orig"
	srvCache := tmp + "/server-cache"
	clientCache := tmp + "/client-cache"

	os.Mkdir(mnt, 0700)
	os.Mkdir(orig, 0700)
	os.Mkdir(orig+"/subdir", 0700)
	content := "hello"
	err := ioutil.WriteFile(orig+"/file.txt", []byte(content), 0644)
	if err != nil {
		t.Fatal(err)
	}

	cache := NewContentCache(srvCache)
	server := NewFsServer(orig, cache, []string{})

	l, r, err := fuse.Socketpair("unix")
	if err != nil {
		t.Fatal(err)
	}

	rpcServer := rpc.NewServer()
	rpcServer.Register(server)
	go rpcServer.ServeConn(l)

	rpcClient := rpc.NewClient(r)
	fs := NewRpcFs(rpcClient, NewContentCache(clientCache))

	state, _, err := fuse.MountFileSystem(mnt, fs, nil)
	state.Debug = true
	if err != nil {
		t.Fatal("Mount", err)
	}
	defer func() {
		log.Println("unmounting")
		err := state.Unmount()
		if err == nil {
			os.RemoveAll(tmp)
		}
	}()

	go state.Loop(false)

	fi, err := os.Lstat(mnt + "/subdir")
	if fi == nil || !fi.IsDirectory() {
		t.Fatal("subdir stat", fi, err)
	}

	c, err := ioutil.ReadFile(mnt + "/file.txt")
	if err != nil || string(c) != "hello" {
		t.Error("Readfile", c)
	}

	entries, err := ioutil.ReadDir(mnt)
	if err != nil || len(entries) != 2 {
		t.Error("Readdir", err, entries)
	}

	// This test implementation detail - should be separate?
	storedHash := server.hashCache["/file.txt"]
	if storedHash == nil || string(storedHash) != string(md5str(content)) {
		t.Errorf("cache error %x (%v)", storedHash, storedHash)
	}

	newData := []FileAttr{
		FileAttr{
			Path: "/file.txt",
			Hash: md5str("somethingelse"),
		},
	}
	server.updateHashes(newData)
	storedHash = server.hashCache["/file.txt"]
	if storedHash == nil || string(storedHash) != string(newData[0].Hash) {
		t.Errorf("cache error %x (%v)", storedHash, storedHash)
	}
}
