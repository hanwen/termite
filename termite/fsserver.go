package termite

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

// TODO - should have a path -> md5 cache so we can answer the 2nd
// getattr quickly.
type FsServer struct {
	contentServer *ContentServer
	cache         *ContentCache
	mutableRoot   string
	exportedRoot  string
	excluded      map[string]bool
}

func NewFsServer(mutableRoot string, cache *ContentCache, excluded []string) *FsServer {
	fs := &FsServer{
		cache:         cache,
		contentServer: &ContentServer{Cache: cache},
		mutableRoot:   mutableRoot,
		exportedRoot:  "/",
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
	Path string // Used in WorkReply
	*os.FileInfo
	fuse.Status
	Hash    []byte
	Link    string
	Content []byte		// optional.
}

func (me AttrResponse) String() string {
	id := ""
	if me.Hash != nil {
		id = fmt.Sprintf(" sz %d", me.FileInfo.Size)
	}
	if me.Link != "" {
		id = fmt.Sprintf(" -> %s", me.Link)
	}
	if me.Deletion() {
		id = " (del)"
	}
	return fmt.Sprintf("%s%s", me.Path, id)
}

func (me AttrResponse) Deletion() bool {
	return me.Status == fuse.ENOENT
}

type DirRequest struct {
	Name string
}

type DirResponse struct {
	NameModeMap map[string]uint32
}

func (me *FsServer) path(n string) string {
	if me.exportedRoot == "" {
		return n
	}
	return filepath.Join(me.exportedRoot, strings.TrimLeft(n, "/"))
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
		if strings.HasPrefix(req.Name, me.mutableRoot) {
			rep.Hash, rep.Content = me.cache.SavePath(req.Name)
		} 
	}
	log.Println("GetAttr", req.Name, rep)
	return nil
}

