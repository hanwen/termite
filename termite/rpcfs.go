package termite

import (
	"fmt"
	"github.com/hanwen/go-fuse/fuse"
	"log"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type RpcFs struct {
	fuse.DefaultFileSystem
	cache  *ContentCache
	client *rpc.Client

	// Roots that we should try to fetch locally.
	localRoots []string
	timings    *TimerStats
	attr       *AttributeCache
	id         string

	// Below code is used to make sure we only fetch each hash
	// once.
	mutex    sync.Mutex
	cond     *sync.Cond
	fetching map[string]bool
}

func NewRpcFs(server *rpc.Client, cache *ContentCache) *RpcFs {
	me := &RpcFs{}
	me.client = server
	me.timings = NewTimerStats()
	me.attr = NewAttributeCache(
		func(n string) *FileAttr {
			return me.fetchAttr(n)
		}, nil)
	me.cond = sync.NewCond(&me.mutex)
	me.fetching = map[string]bool{}
	me.cache = cache
	return me
}

func (me *RpcFs) Close() {
	me.client.Close()
}

func (me *RpcFs) innerFetch(req *ContentRequest, rep *ContentResponse) error {
	start := time.Nanoseconds()
	err := me.client.Call("FsServer.FileContent", req, rep)
	dt := time.Nanoseconds() - start
	me.timings.Log("FsServer.FileContent", dt)
	me.timings.LogN("FsServer.FileContentBytes", int64(len(rep.Chunk)), dt)
	return err
}

func (me *RpcFs) FetchHash(a *FileAttr) error {
	e := me.FetchHashOnce(a)
	if e == nil && a.Size < _MEMORY_LIMIT {
		me.cache.FaultIn(a.Hash)
	}
	return e
}

func (me *RpcFs) FetchHashOnce(a *FileAttr) error {
	me.mutex.Lock()
	defer me.mutex.Unlock()
	h := a.Hash
	for !me.cache.HasHash(h) && me.fetching[h] {
		me.cond.Wait()
	}
	if me.cache.HasHash(h) {
		return nil
	}
	me.fetching[h] = true
	me.mutex.Unlock()
	log.Printf("Fetching contents for file %s: %x", a.Path, h)
	err := me.cache.FetchFromServer(
		func(req *ContentRequest, rep *ContentResponse) error {
			return me.innerFetch(req, rep)
		}, h)
	me.mutex.Lock()
	delete(me.fetching, h)
	me.cond.Broadcast()
	return err
}

func (me *RpcFs) Update(req *UpdateRequest, resp *UpdateResponse) error {
	me.updateFiles(req.Files)
	return nil
}

func (me *RpcFs) updateFiles(files []*FileAttr) {
	me.attr.Update(files)
}

func (me *RpcFs) fetchAttr(n string) *FileAttr {
	req := &AttrRequest{
		Name:   n,
		Origin: me.id,
	}
	start := time.Nanoseconds()
	rep := &AttrResponse{}
	err := me.client.Call("FsServer.GetAttr", req, rep)
	dt := time.Nanoseconds() - start
	me.timings.Log("FsServer.GetAttr", dt)
	if err != nil {
		// fatal?
		log.Println("GetAttr error:", err)
		return nil
	}

	var wanted *FileAttr
	for _, attr := range rep.Attrs {
		if attr.Path == n {
			wanted = attr
		}
	}

	// TODO - if we got a deletion, we should refetch the parent.
	return wanted
}

func (me *RpcFs) considerSaveLocal(attr *FileAttr) {
	absPath := attr.Path
	if attr.Deletion() || !attr.FileInfo.IsRegular() {
		return
	}
	found := false
	for _, root := range me.localRoots {
		if HasDirPrefix(absPath, root) {
			found = true
		}
	}
	if !found {
		return
	}

	fi, _ := os.Lstat(absPath)
	if fi == nil {
		return
	}
	if EncodeFileInfo(*fi) != EncodeFileInfo(*attr.FileInfo) {
		return
	}
}

////////////////////////////////////////////////////////////////
// FS API

func (me *RpcFs) String() string {
	return "RpcFs"
}

func (me *RpcFs) OpenDir(name string, context *fuse.Context) (chan fuse.DirEntry, fuse.Status) {
	r := me.attr.GetDir(name)
	if r.Deletion() {
		return nil, fuse.ENOENT
	}
	if !r.FileInfo.IsDirectory() {
		return nil, fuse.EINVAL
	}

	c := make(chan fuse.DirEntry, len(r.NameModeMap))
	for k, mode := range r.NameModeMap {
		c <- fuse.DirEntry{
			Name: k,
			Mode: uint32(mode),
		}
	}
	close(c)
	return c, fuse.OK
}

type rpcFsFile struct {
	fuse.File
	os.FileInfo
}

func (me *rpcFsFile) GetAttr() (*os.FileInfo, fuse.Status) {
	return &me.FileInfo, fuse.OK
}

func (me *rpcFsFile) String() string {
	return fmt.Sprintf("rpcFsFile(%s)", me.File.String())
}

func (me *RpcFs) Open(name string, flags uint32, context *fuse.Context) (fuse.File, fuse.Status) {
	if flags&fuse.O_ANYWRITE != 0 {
		return nil, fuse.EPERM
	}
	a := me.attr.Get(name)
	if a == nil {
		return nil, fuse.ENOENT
	}
	if a.Deletion() {
		return nil, fuse.ENOENT
	}

	if err := me.FetchHash(a); err != nil {
		log.Printf("Error fetching contents %v", err)
		return nil, fuse.EIO
	}

	if contents := me.cache.ContentsIfLoaded(a.Hash); contents != nil {
		return &fuse.WithFlags{
			File: &rpcFsFile{
				fuse.NewDataFile(contents),
				*a.FileInfo,
			},
			FuseFlags: fuse.FOPEN_KEEP_CACHE,
		}, fuse.OK
	}
	return &fuse.WithFlags{
		File: &rpcFsFile{
			&LazyLoopbackFile{Name: me.cache.Path(a.Hash)},
			*a.FileInfo,
		},
		FuseFlags: fuse.FOPEN_KEEP_CACHE,
	}, fuse.OK
}

func (me *RpcFs) Readlink(name string, context *fuse.Context) (string, fuse.Status) {
	a := me.attr.Get(name)
	if a == nil {
		return "", fuse.ENOENT
	}

	if a.Deletion() {
		return "", fuse.ENOENT
	}
	if !a.FileInfo.IsSymlink() {
		return "", fuse.EINVAL
	}

	return a.Link, fuse.OK
}

func (me *RpcFs) GetAttr(name string, context *fuse.Context) (*os.FileInfo, fuse.Status) {
	r := me.attr.Get(name)
	if r == nil {
		return nil, fuse.ENOENT
	}
	if r.Hash != "" {
		go me.FetchHash(r)
	}
	return r.FileInfo, r.Status()
}

func (me *RpcFs) Access(name string, mode uint32, context *fuse.Context) (code fuse.Status) {
	if mode == fuse.F_OK {
		_, code := me.GetAttr(name, context)
		return code
	}
	if mode&fuse.W_OK != 0 {
		return fuse.EACCES
	}
	return fuse.OK
}
