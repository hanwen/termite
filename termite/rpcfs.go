package termite

import (
	"os"
	"log"
	"sync"
	"rpc"
	"github.com/hanwen/go-fuse/fuse"
	"strings"
	"path/filepath"
)

type RpcFs struct {
	fuse.DefaultFileSystem
	cache *ContentCache

	client *rpc.Client

	// Should be acquired before attrMutex if applicable.
	dirMutex    sync.Mutex
	directories map[string]*DirResponse

	attrMutex    sync.RWMutex
	attrResponse map[string]*AttrResponse

	fetchMutex sync.Mutex
	fetchCond  sync.Cond
	fetchMap   map[string]bool
}

type UpdateRequest struct {
	FileServer   string
	WritableRoot string
	Files        []AttrResponse
}

type UpdateResponse struct {

}

func NewRpcFs(server *rpc.Client, cache *ContentCache) *RpcFs {
	me := &RpcFs{}
	me.client = server
	me.directories = make(map[string]*DirResponse)
	me.cache = cache
	me.attrResponse = make(map[string]*AttrResponse)

	me.fetchMap = make(map[string]bool)
	me.fetchCond.L = &me.fetchMutex
	return me
}

func (me *RpcFs) Update(req *UpdateRequest, resp *UpdateResponse) os.Error {
	me.dirMutex.Lock()
	defer me.dirMutex.Unlock()

	me.attrMutex.Lock()
	defer me.attrMutex.Unlock()

	flushDirs := []string{}
	for _, r := range req.Files {
		p := strings.TrimLeft(r.Path, string(filepath.Separator))

		d, _ := filepath.Split(p)
		a, existed := me.attrResponse[p]

		if r.Deletion() || (existed && a.Status != fuse.ENOENT) {
			flushDirs = append(flushDirs, d)
		}

		if r.Deletion() {
			me.attrResponse[p] = nil, false
		} else {
			newVal := r
			me.attrResponse[p] = &newVal
		}
	}

	for _, d := range flushDirs {
		me.directories[d] = nil, false
	}
	return nil
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
	if a == nil {
		return nil, fuse.ENOENT
	}
	if !a.Status.Ok() {
		return nil, a.Status
	}

	p := me.cache.Path(a.Hash)
	if _, err := os.Lstat(p); fuse.OsErrorToErrno(err) == fuse.ENOENT {
		log.Printf("Fetching contents for file %s", name)
		err = me.FetchHash(a.FileInfo.Size, a.Hash)
		// should return something else?
		if err != nil {
			return nil, fuse.ENOENT
		}
	}

	f, err := os.Open(p)
	if err != nil {
		return nil, fuse.OsErrorToErrno(err)
	}

	return &fuse.LoopbackFile{File: f}, fuse.OK
}

func (me *RpcFs) FetchHash(size int64, hash []byte) os.Error {
	key := string(hash)
	me.fetchMutex.Lock()
	defer me.fetchMutex.Unlock()
	for me.fetchMap[key] && !me.cache.HasHash(hash) {
		me.fetchCond.Wait()
	}

	if me.cache.HasHash(hash) {
		return nil
	}
	me.fetchMap[key] = true
	defer func() {
		me.fetchMap[key] = false, false
		me.fetchCond.Signal()
	}()
	me.fetchMutex.Unlock()
	err := me.fetchOnce(size, hash)
	me.fetchMutex.Lock()
	return err
}

func (me *RpcFs) fetchOnce(size int64, hash []byte) os.Error {
	// TODO - should save in smaller chunks.
	return FetchBetweenContentServers(me.client, "FsServer.FileContent", size, hash,
		me.cache)
}

func (me *RpcFs) Readlink(name string) (string, fuse.Status) {
	a := me.getAttrResponse(name)
	if a == nil {
		return "", fuse.ENOENT
	}

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
	if r == nil {
		return nil, fuse.ENOENT
	}

	return r.FileInfo, r.Status
}
