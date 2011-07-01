package termite

import (
	"bytes"
	"os"
	"log"
	"sync"
	"rpc"
	"github.com/hanwen/go-fuse/fuse"
)

type RpcFs struct {
	fuse.DefaultFileSystem
	cache *DiskFileCache

	client *rpc.Client

	dirMutex    sync.Mutex
	directories map[string]*DirResponse

	attrMutex    sync.RWMutex
	attrResponse map[string]*AttrResponse
}

func NewRpcFs(server *rpc.Client, cache *DiskFileCache) *RpcFs {
	me := &RpcFs{}
	me.client = server
	me.directories = make(map[string]*DirResponse)
	me.cache = cache
	me.attrResponse = make(map[string]*AttrResponse)
	return me
}

func (me *RpcFs) GetDir(name string) *DirResponse {
	me.dirMutex.Lock()
	defer me.dirMutex.Unlock()

	r, ok := me.directories[name]
	if ok {
		return r
	}

	// TODO - asynchronous.
	// TODO - eliminate cut & paste
	req := &DirRequest{Name: "/" + name}
	rep := &DirResponse{}
	err := me.client.Call("FsServer.ReadDir", req, rep)
	if err != nil {
		log.Println("GetDir error:", err)
		return nil
	}

	me.directories[name] = rep
	return rep
}

func (me *RpcFs) OpenDir(name string) (chan fuse.DirEntry, fuse.Status) {
	r := me.GetDir(name)

	if r == nil {
		return nil, fuse.ENOENT
	}
	c := make(chan fuse.DirEntry, len(r.NameModeMap))
	for k, mode := range r.NameModeMap {
		c <- fuse.DirEntry{
			Name: k,
			Mode: mode,
		}
	}
	close(c)
	return c, fuse.OK
}

func (me *RpcFs) Open(name string, flags uint32) (fuse.File, fuse.Status) {
	if flags&fuse.O_ANYWRITE != 0 {
		return nil, fuse.EPERM
	}
	a := me.getAttrResponse(name)
	if !a.Status.Ok() {
		return nil, a.Status
	}

	p := me.cache.Path(a.Hash)
	if _, err := os.Lstat(p); fuse.OsErrorToErrno(err) == fuse.ENOENT {
		log.Printf("Fetching contents for file %s", name)
		me.FetchHash(a.FileInfo.Size, a.Hash)
	}

	f, err := os.Open(p)
	if err != nil {
		return nil, fuse.OsErrorToErrno(err)
	}

	return &fuse.LoopbackFile{File: f}, fuse.OK
}

// TODO - should be streaming.
func (me *RpcFs) FetchHash(size int64, hash []byte) {
	b := FetchFromContentServer(me.client, "FsServer.FileContent", size, hash)
	savedHash := me.cache.Save(b)
	if bytes.Compare(hash, savedHash) != 0 {
		log.Fatalf("Corruption: savedHash %x != requested hash %x.", savedHash, hash)
	}
}

func (me *RpcFs) Readlink(name string) (string, fuse.Status) {
	a := me.getAttrResponse(name)

	if !a.Status.Ok() {
		return "", a.Status
	}
	if !a.FileInfo.IsSymlink() {
		return "", fuse.EINVAL
	}

	return a.Link, fuse.OK
}

func (me *RpcFs) getAttrResponse(name string) *AttrResponse {
	me.attrMutex.RLock()
	result, ok := me.attrResponse[name]
	me.attrMutex.RUnlock()

	if ok {
		return result
	}

	req := &AttrRequest{Name: "/" + name}
	rep := &AttrResponse{}
	err := me.client.Call("FsServer.GetAttr", req, rep)
	if err != nil {
		log.Println("GetAttr error:", err)
		return nil
	}

	me.attrMutex.Lock()
	defer me.attrMutex.Unlock()
	me.attrResponse[name] = rep
	return rep
}

func (me *RpcFs) GetAttr(name string) (*os.FileInfo, fuse.Status) {
	if name == "" {
		return &os.FileInfo{
			Mode: fuse.S_IFDIR | 0755,
		}, fuse.OK
	}

	r := me.getAttrResponse(name)
	return r.FileInfo, r.Status
}
