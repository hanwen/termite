package termite

import (
	"fmt"
	"io"
	"log"

	"github.com/hanwen/go-fuse/fuse"
	"github.com/hanwen/go-fuse/fuse/nodefs"
	"github.com/hanwen/go-fuse/fuse/pathfs"
	"github.com/hanwen/termite/attr"
	"github.com/hanwen/termite/cba"
	"github.com/hanwen/termite/stats"
)

type RpcFs struct {
	pathfs.FileSystem
	cache         *cba.Store
	attrClient    *attr.Client
	contentClient *cba.Client

	timings *stats.TimerStats
	attr    *attr.AttributeCache
	id      string
}

func NewRpcFs(attrClient *attr.Client, cache *cba.Store, contentConn io.ReadWriteCloser) *RpcFs {
	fs := &RpcFs{
		FileSystem:    pathfs.NewDefaultFileSystem(),
		attrClient:    attrClient,
		contentClient: cache.NewClient(contentConn),
		timings:       stats.NewTimerStats(),
	}

	fs.attr = attr.NewAttributeCache(
		func(n string) *attr.FileAttr {
			a := attr.FileAttr{}
			err := attrClient.GetAttr(n, &a)
			if err != nil {
				log.Printf("GetAttr %s: %v", n, err)
				return nil
			}
			return &a
		}, nil)
	fs.cache = cache
	return fs
}

func (fs *RpcFs) Close() {
	fs.attrClient.Close()
	fs.contentClient.Close()
}

func (fs *RpcFs) FetchHash(a *attr.FileAttr) error {
	got, e := fs.contentClient.FetchOnce(a.Hash, int64(a.Size))
	if e == nil && !got {
		log.Fatalf("Did not have hash %x for %s", a.Hash, a.Path)
	}
	return e
}

func (fs *RpcFs) Update(req *UpdateRequest, resp *UpdateResponse) error {
	fs.updateFiles(req.Files)
	return nil
}

func (fs *RpcFs) updateFiles(files []*attr.FileAttr) {
	fs.attr.Update(files)
}

////////////////////////////////////////////////////////////////
// FS API

func (fs *RpcFs) String() string {
	return "RpcFs"
}

func (fs *RpcFs) OpenDir(name string, context *fuse.Context) ([]fuse.DirEntry, fuse.Status) {
	r := fs.attr.GetDir(name)
	if r.Deletion() {
		return nil, fuse.ENOENT
	}
	if !r.IsDir() {
		return nil, fuse.EINVAL
	}

	c := make([]fuse.DirEntry, 0, len(r.NameModeMap))
	for k, mode := range r.NameModeMap {
		c = append(c, fuse.DirEntry{
			Name: k,
			Mode: uint32(mode),
		})
	}
	return c, fuse.OK
}

func hashIno(in string) uint64 {
	return uint64(in[0]) | uint64(in[1])<<8 | uint64(in[2])<<16 | uint64(in[3])<<24 | uint64(in[4])<<32 | uint64(in[5])<<40 | uint64(in[6])<<48 | uint64(in[7])<<56
}

type rpcFsFile struct {
	nodefs.File

	attr fuse.Attr
	hash string
}

func (f *rpcFsFile) GetAttr(a *fuse.Attr) fuse.Status {
	*a = f.attr
	if a.Size > 0 {
		a.Ino = hashIno(f.hash)
	}
	return fuse.OK
}

func (fs *rpcFsFile) String() string {
	return fmt.Sprintf("rpcFsFile(%s)", fs.File.String())
}

func (fs *RpcFs) Open(name string, flags uint32, context *fuse.Context) (nodefs.File, fuse.Status) {
	if flags&fuse.O_ANYWRITE != 0 {
		return nil, fuse.EPERM
	}
	a := fs.attr.Get(name)
	if a == nil {
		return nil, fuse.ENOENT
	}
	if a.Deletion() {
		return nil, fuse.ENOENT
	}

	if err := fs.FetchHash(a); err != nil {
		log.Printf("Error fetching contents %v", err)
		return nil, fuse.EIO
	}

	fa := *a.Attr
	return &nodefs.WithFlags{
		File: &rpcFsFile{
			File: NewLazyLoopbackFile(fs.cache.Path(a.Hash)),
			attr: fa,
			hash: a.Hash,
		},
		FuseFlags: fuse.FOPEN_KEEP_CACHE,
	}, fuse.OK
}

func (fs *RpcFs) Readlink(name string, context *fuse.Context) (string, fuse.Status) {
	a := fs.attr.Get(name)
	if a == nil {
		return "", fuse.ENOENT
	}

	if a.Deletion() {
		return "", fuse.ENOENT
	}
	if !a.IsSymlink() {
		return "", fuse.EINVAL
	}

	// TODO - kick off getattr on destination.
	return a.Link, fuse.OK
}

func (fs *RpcFs) GetAttr(name string, context *fuse.Context) (*fuse.Attr, fuse.Status) {
	r := fs.attr.Get(name)
	if r == nil {
		return nil, fuse.ENOENT
	}
	if r.Hash != "" {
		go fs.FetchHash(r)
	}
	a := &fuse.Attr{}
	if r.Deletion() {
		a = nil
	} else {
		a = r.Attr
		if r.Hash != "" && a.Size > 0 {
			a.Ino = hashIno(r.Hash)
		} else {
			// Clear out inode, so pathfs does
			// not get confused.
			a.Ino = 0
		}
	}

	return a, r.Status()
}

func (fs *RpcFs) Access(name string, mode uint32, context *fuse.Context) (code fuse.Status) {
	if mode == fuse.F_OK {
		_, code := fs.GetAttr(name, context)
		return code
	}
	if mode&fuse.W_OK != 0 {
		return fuse.EACCES
	}
	return fuse.OK
}
