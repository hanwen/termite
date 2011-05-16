package rpcfs

import (
	"os"
	"path/filepath"
	"log"
	"sync"
	"rpc"
	"github.com/hanwen/go-fuse/fuse"
)

type RpcFs struct {
	fuse.DefaultFileSystem

	client *rpc.Client

	dirMutex sync.Mutex
	directories map[string]*DirResponse

	contentMutex sync.Mutex
	contents map[string][]byte

	linksMutex sync.Mutex
	links map[string]string
}

func NewRpcFs(server string) *RpcFs {
	me := &RpcFs{}
	client, err := rpc.DialHTTP("tcp", server)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	me.client = client
	me.directories = make(map[string]*DirResponse)
	me.contents = make(map[string][]byte)
	me.links = make(map[string]string)
	return me
}

func (me *RpcFs) GetContents(name string) []byte {
	me.contentMutex.Lock()
	defer me.contentMutex.Unlock()

	data, ok := me.contents[name]
	if ok {
		return data
	}

	// TODO - asynchronous.
	req := &ContentRequest{
	Name: name,
	}
	rep := &ContentResponse{}
	err := me.client.Call("FsServer.FileContent", &req, &rep)
	if err != nil {
		log.Println("ReadFile error", err)
		return nil
	}
	me.contents[name] = rep.Data
	return rep.Data
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
	c := make(chan fuse.DirEntry, len(r.Data))
	for k, fi := range r.Data {
		c <- fuse.DirEntry{
		Name: k,
		Mode: fi.Mode,
		}
	}
	close(c)
	return c, fuse.OK
}

func (me *RpcFs) Open(name string, flags uint32) (fuse.File, fuse.Status) {
	c := me.GetContents(name)
	if c == nil {
		return nil, fuse.ENOENT
	}
	return fuse.NewReadOnlyFile(c), fuse.OK
}


func (me *RpcFs) Readlink(name string) (string, fuse.Status) {
	v := me.Getlink(name)
	if v == nil {
		return "", fuse.ENOENT
	}
	return *v, fuse.OK
}

func (me *RpcFs) Getlink(name string) (*string) {
	me.linksMutex.Lock()
	defer me.linksMutex.Unlock()

	r, ok := me.links[name]
	if ok {
		return &r
	}
	
	// TODO - asynchronous.
	req := &LinkRequest{Name: "/" + name}
	rep := &LinkResponse{}
	err := me.client.Call("FsServer.Readlink", req, rep)
	if err != nil {
		log.Println("GetLink error:", err)
		return nil
	}

	me.links[name] = rep.Data
	return &rep.Data
}
	
func (me *RpcFs) GetAttr(name string) (*os.FileInfo, fuse.Status) {
	if name == "" {
		return &os.FileInfo{
		Mode: fuse.S_IFDIR | 0755,
		}, fuse.OK
	}

	dir, base := filepath.Split(name)
	d := me.GetDir(dir)
	if d == nil {
		return nil, fuse.ENOENT
	}

	fi, ok := d.Data[base]
	if !ok {
		return nil, fuse.ENOENT
	}

	return fi, fuse.OK
}
