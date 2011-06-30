package rpcfs

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"io/ioutil"
	"strings"
	"github.com/hanwen/go-fuse/fuse"
)

var _ = fmt.Println

type FsServer struct {
	cache *DiskFileCache
	
	Root string
}

func NewFsServer(root, cachedir string) *FsServer {
	return &FsServer{
	cache: NewDiskFileCache(cachedir),
	Root: root,
	}
}


type Stamp struct {
	Ctime int64
	Mtime int64
}

type ContentRequest struct {
	Hash []byte
	Start, End int
}

type ContentResponse struct {
	Chunk []byte
}

type AttrRequest struct {
	Name string 
}

type AttrResponse struct {
	*os.FileInfo
	fuse.Status
	Hash []byte
	Link string
}

type DirRequest struct {
	Name string 
}

type DirResponse struct {
	NameModeMap map[string]uint32
}

func (me *FsServer) Path(n string) string {
	if me.Root == "" {
		return n
	}
	return filepath.Join(me.Root, strings.TrimLeft(n, "/"))
}

func (me *FsServer) ReadDir(req *DirRequest, r *DirResponse) (os.Error) {
	d, e :=  ioutil.ReadDir(me.Path(req.Name))
	log.Println("ReadDir", req)
	r.NameModeMap = make(map[string]uint32)
	for _, v := range d {
		r.NameModeMap[v.Name] = v.Mode
	}
	return e
}

func (me *FsServer) FileContent(req *ContentRequest, rep *ContentResponse) (os.Error) {
	f, err := os.Open(HashPath(me.cache.dir, req.Hash))
	if err != nil {
		return err
	}
	defer f.Close()

	rep.Chunk = make([]byte, req.End-req.Start)
	log.Println(req)
	n, err := f.ReadAt(rep.Chunk, int64(req.Start))
	rep.Chunk = rep.Chunk[:n]
	
	log.Println(n)
	if err == os.EOF {
		err = nil
	}
	return err
}


func (me *FsServer) GetAttr(req *AttrRequest, rep *AttrResponse) (os.Error) {
	fi, err := os.Lstat(me.Path(req.Name))
	rep.FileInfo = fi
	rep.Status = fuse.OsErrorToErrno(err)
	if fi == nil {
		return nil
	}
	
	if fi.IsSymlink() {
		rep.Link, err = os.Readlink(req.Name)
	}
	if fi.IsRegular() {
		rep.Hash = me.cache.SavePath(req.Name)
	}
	log.Println("GetAttr", req, rep)
	return nil
}
