package termite

// This file tests both rpcfs and fsserver by having them talk over a
// socketpair.

import (
	"crypto"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/rpc"
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

func (tc *rpcFsTestCase) getattr(n string) *attr.FileAttr {
	p := filepath.Join(tc.orig, n)
	a := GetattrForTest(tc.tester, p)
	if a.Hash != "" {
		tc.serverStore.SavePath(p)
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

func newRpcFsTestCase(t *testing.T) (tc *rpcFsTestCase) {
	tc = &rpcFsTestCase{tester: t}
	tc.tmp, _ = ioutil.TempDir("", "term-fss")

	tc.mnt = tc.tmp + "/mnt"
	tc.orig = tc.tmp + "/orig"
	srvCache := tc.tmp + "/server-cache"

	os.Mkdir(tc.mnt, 0700)
	os.Mkdir(tc.orig, 0700)

	copts := cba.StoreOptions{
		Dir: srvCache,
	}
	tc.serverStore = cba.NewStore(&copts)
	tc.attr = attr.NewAttributeCache(
		func(n string) *attr.FileAttr { return tc.getattr(n) },
		func(n string) *fuse.Attr {
			return StatForTest(t, filepath.Join(tc.orig, n))
		})
	tc.attr.Paranoia = true
	tc.server = attr.NewServer(tc.attr)

	var err error
	tc.sockL, tc.sockR, err = netPair()
	if err != nil {
		t.Fatal(err)
	}
	tc.contentL, tc.contentR, err = netPair()
	if err != nil {
		t.Fatal(err)
	}

	rpcServer := rpc.NewServer()
	rpcServer.Register(tc.server)
	go func() {
		rpcServer.ServeConn(tc.sockL)
		tc.sockL.Close()
	}()
	go func() {
		tc.serverStore.ServeConn(tc.contentL)
		tc.contentL.Close()
	}()

	cOpts := cba.StoreOptions{
		Dir: tc.tmp + "/client-cache",
	}
	tc.clientStore = cba.NewStore(&cOpts)
	attrClient := attr.NewClient(tc.sockR, "id")
	tc.rpcFs = NewRpcFs(attrClient, tc.clientStore, tc.contentR)
	tc.rpcFs.id = "rpcfs_test"
	nfs := pathfs.NewPathNodeFs(tc.rpcFs, nil)
	tc.state, _, err = nodefs.MountRoot(tc.mnt, nfs.Root(), nil)
	//	tc.state.SetDebug(fuse.VerboseTest())
	if err != nil {
		t.Fatal("Mount", err)
	}

	go tc.state.Serve()
	return tc
}

func (tc *rpcFsTestCase) Clean() {
	if err := tc.state.Unmount(); err != nil {
		log.Panic("fuse unmount failed.", err)
	}
	os.RemoveAll(tc.tmp)
	tc.contentR.Close()
	tc.sockR.Close()
}

func check(err error) {
	if err != nil {
		panic(err)
	}
}

func TestRpcFsReadDirCache(t *testing.T) {
	tc := newRpcFsTestCase(t)
	defer tc.Clean()

	os.Mkdir(tc.orig+"/subdir", 0700)
	content := "hello"
	err := ioutil.WriteFile(tc.orig+"/subdir/file.txt", []byte(content), 0644)
	check(err)

	entries, err := ioutil.ReadDir(tc.mnt + "/subdir")
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

	before, _ := os.Lstat(tc.orig + "/subdir")
	for {
		err = ioutil.WriteFile(tc.orig+"/subdir/unstatted.txt", []byte("somethingelse"), 0644)
		check(err)
		after, _ := os.Lstat(tc.orig + "/subdir")
		if !before.ModTime().Equal(after.ModTime()) {
			break
		}
		time.Sleep(10e6)
		os.Remove(tc.orig + "/subdir/unstatted.txt")
	}

	err = os.Remove(tc.orig + "/subdir/file.txt")
	check(err)

	fset := tc.attr.Refresh("")
	tc.rpcFs.updateFiles(fset.Files)

	_, err = ioutil.ReadDir(tc.mnt + "/subdir")
	check(err)

	dir := tc.rpcFs.attr.GetDir("subdir")
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
	tc := newRpcFsTestCase(t)
	defer tc.Clean()

	os.Mkdir(tc.orig+"/subdir", 0700)
	content := "hello"
	err := ioutil.WriteFile(tc.orig+"/file.txt", []byte(content), 0644)
	if err != nil {
		t.Fatal(err)
	}

	fi, err := os.Lstat(tc.mnt + "/subdir")
	if fi == nil || !fi.IsDir() {
		t.Fatal("subdir stat", fi, err)
	}

	c, err := ioutil.ReadFile(tc.mnt + "/file.txt")
	if err != nil || string(c) != "hello" {
		t.Errorf("Readfile: want 'hello', got '%s', err %v", c, err)
	}

	entries, err := ioutil.ReadDir(tc.mnt)
	if err != nil || len(entries) != 2 {
		t.Error("Readdir", err, entries)
	}

	// This test implementation detail - should be separate?
	a := tc.attr.Get("file.txt")
	if a == nil || a.Hash == "" || string(a.Hash) != string(md5str(content)) {
		t.Errorf("cache error %v (%x)", a)
	}

	newcontent := "somethingelse"
	err = ioutil.WriteFile(tc.orig+"/file.txt", []byte(newcontent), 0644)
	check(err)
	err = ioutil.WriteFile(tc.orig+"/foobar.txt", []byte("more content"), 0644)
	check(err)

	tc.attr.Refresh("")
	a = tc.attr.Get("file.txt")
	if a == nil || a.Hash == "" || a.Hash != md5str(newcontent) {
		t.Errorf("refreshAttributeCache: cache error got %v, want %x", a, md5str(newcontent))
	}
}

func TestRpcFsFetchOnce(t *testing.T) {
	tc := newRpcFsTestCase(t)
	defer tc.Clean()
	ioutil.WriteFile(tc.orig+"/file.txt", []byte{42}, 0644)
	tc.attr.Refresh("")

	ioutil.ReadFile(tc.mnt + "/file.txt")

	stats := tc.serverStore.TimingMap()
	key := "ContentStore.Save"
	if stats == nil || stats[key] == nil {
		t.Fatalf("Stats %q missing: %v", key, stats)
	}
	if stats[key].N > 1 {
		t.Errorf("File content was served more than once.")
	}
}

func TestFsServerCache(t *testing.T) {
	tc := newRpcFsTestCase(t)
	defer tc.Clean()

	content := "hello"
	err := ioutil.WriteFile(tc.orig+"/file.txt", []byte(content), 0644)
	tc.attr.Refresh("")
	c := tc.attr.Copy().Files
	if len(c) > 0 {
		t.Errorf("cache not empty? %#v", c)
	}

	os.Lstat(tc.mnt + "/file.txt")
	c = tc.attr.Copy().Files
	if len(c) != 2 {
		t.Errorf("cache should have 2 entries, got %#v", c)
	}
	name := "file.txt"
	ok := tc.attr.Have(name)
	if !ok {
		t.Errorf("no entry for %q", name)
	}

	newName := tc.orig + "/new.txt"
	err = os.Rename(tc.orig+"/file.txt", newName)
	if err != nil {
		t.Fatal(err)
	}

	tc.attr.Refresh("")
	ok = tc.attr.Have(name)
	if ok {
		t.Errorf("after rename: entry for %q unexpected", name)
	}
}
