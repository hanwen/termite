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
	contentServer *ContentServer
	cache         *DiskFileCache
	Root          string
	excluded      map[string]bool
}

func NewFsServer(root string, cache *DiskFileCache, excluded []string) *FsServer {
	fs := &FsServer{
		cache:         cache,
		contentServer: &ContentServer{Cache: cache},
		Root:          root,
	}

	fs.excluded = make(map[string]bool)
	for _, e := range excluded {
		fs.excluded[e] = true
	}
	return fs
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

func (me *FsServer) path(n string) string {
	if me.Root == "" {
		return n
	}
	return filepath.Join(me.Root, strings.TrimLeft(n, "/"))
}

func (me *FsServer) FileContent(req *ContentRequest, rep *ContentResponse) os.Error {
	return me.contentServer.FileContent(req, rep)
}

func (me *FsServer) ReadDir(req *DirRequest, r *DirResponse) os.Error {
	d, e := ioutil.ReadDir(me.path(req.Name))
	log.Println("ReadDir", req)
	r.NameModeMap = make(map[string]uint32)
	for _, v := range d {
		r.NameModeMap[v.Name] = v.Mode
	}
	return e
}

func (me *FsServer) GetAttr(req *AttrRequest, rep *AttrResponse) os.Error {
	log.Println("GetAttr req", req.Name)
	if me.excluded[req.Name] {
		rep.Status = fuse.ENOENT
		return nil
	}

	fi, err := os.Lstat(me.path(req.Name))
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
	log.Println("GetAttr", req.Name, rep)
	return nil
}
