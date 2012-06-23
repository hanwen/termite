package termite

import (
	"fmt"
	"github.com/hanwen/go-fuse/fuse"
	"github.com/hanwen/go-fuse/raw"
	"github.com/hanwen/termite/attr"
	"github.com/hanwen/termite/cba"
	"github.com/hanwen/termite/stats"
	"io"
	"log"
)

type RpcFs struct {
	fuse.DefaultFileSystem
	cache         *cba.Store
	attrClient     *attr.Client
	contentClient *cba.Client

	timings    *stats.TimerStats
	attr       *attr.AttributeCache
	id         string
}

func NewRpcFs(attrClient *attr.Client, cache *cba.Store, contentConn io.ReadWriteCloser) *RpcFs {
	me := &RpcFs{
		attrClient:    attrClient,
		contentClient: cache.NewClient(contentConn),
		timings:       stats.NewTimerStats(),
	}
	
	me.attr = attr.NewAttributeCache(
		func(n string) *attr.FileAttr {
			a := attr.FileAttr{}
			err := attrClient.GetAttr(n, &a)
			if err != nil {
				log.Printf("GetAttr %s: %v", n, err)
				return nil
			}
			return &a
	}, nil)
	me.cache = cache
	return me
}

func (me *RpcFs) Close() {
	me.attrClient.Close()
	me.contentClient.Close()
}

func (me *RpcFs) FetchHash(a *attr.FileAttr) error {
	got, e := me.contentClient.FetchOnce(a.Hash, int64(a.Size))
	if e == nil && !got {
		log.Fatalf("Did not have hash %x for %s", a.Hash, a.Path)
	}
	if e == nil && a.Size < uint64(me.cache.Options.MemMaxSize) {
		me.cache.FaultIn(a.Hash)
	}
	return e
}

func (me *RpcFs) Update(req *UpdateRequest, resp *UpdateResponse) error {
	me.updateFiles(req.Files)
	return nil
}

func (me *RpcFs) updateFiles(files []*attr.FileAttr) {
	me.attr.Update(files)
}

////////////////////////////////////////////////////////////////
// FS API

func (me *RpcFs) String() string {
	return "RpcFs"
}

func (me *RpcFs) OpenDir(name string, context *fuse.Context) ([]fuse.DirEntry, fuse.Status) {
	r := me.attr.GetDir(name)
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

type rpcFsFile struct {
	fuse.File
	fuse.Attr
}

func (me *rpcFsFile) GetAttr(a *fuse.Attr) fuse.Status {
	*a = me.Attr
	return fuse.OK
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
		fa := *a.Attr
		return &fuse.WithFlags{
			File: &rpcFsFile{
				fuse.NewDataFile(contents),
				fa,
			},
			FuseFlags: raw.FOPEN_KEEP_CACHE,
		}, fuse.OK
	}
	fa := *a.Attr
	return &fuse.WithFlags{
		File: &rpcFsFile{
			&LazyLoopbackFile{Name: me.cache.Path(a.Hash)},
			fa,
		},
		FuseFlags: raw.FOPEN_KEEP_CACHE,
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
	if !a.IsSymlink() {
		return "", fuse.EINVAL
	}

	// TODO - kick off getattr on destination.
	return a.Link, fuse.OK
}

func (me *RpcFs) GetAttr(name string, context *fuse.Context) (*fuse.Attr, fuse.Status) {
	r := me.attr.Get(name)
	if r == nil {
		return nil, fuse.ENOENT
	}
	if r.Hash != "" {
		go me.FetchHash(r)
	}
	a := &fuse.Attr{}
	if !r.Deletion() {
		a = r.Attr
	} else {
		a = nil
	}
	return a, r.Status()
}

func (me *RpcFs) Access(name string, mode uint32, context *fuse.Context) (code fuse.Status) {
	if mode == raw.F_OK {
		_, code := me.GetAttr(name, context)
		return code
	}
	if mode&raw.W_OK != 0 {
		return fuse.EACCES
	}
	return fuse.OK
}
