package termite

import (
	"log"
	"github.com/hanwen/termite/attr"
	"github.com/hanwen/termite/cba"
	"github.com/hanwen/termite/stats"
	"time"
)

type FsServer struct {
	contentCache *cba.ContentCache
	attributes    *attr.AttributeCache
	stats        *stats.TimerStats
}

func NewFsServer(a *attr.AttributeCache, cache *cba.ContentCache) *FsServer {
	me := &FsServer{
		contentCache: cache,
		attributes:   a,
		stats:        stats.NewTimerStats(),
	}

	return me
}

func (me *FsServer) FileContent(req *cba.ContentRequest, rep *cba.ContentResponse) error {
	start := time.Nanoseconds()
	err := me.contentCache.Serve(req, rep)
	dt := time.Nanoseconds() - start
	me.stats.Log("FsServer.FileContent", dt)
	me.stats.LogN("FsServer.FileContentBytes", int64(len(rep.Chunk)), dt)
	return err
}

func (me *FsServer) GetAttr(req *AttrRequest, rep *AttrResponse) error {
	start := time.Nanoseconds()
	log.Printf("GetAttr %s req %q", req.Origin, req.Name)
	if req.Name != "" && req.Name[0] == '/' {
		panic("leading /")
	}

	a := me.attributes.GetDir(req.Name)
	if a.Hash != "" {
		log.Printf("GetAttr %v", a)
		if a.Size < me.contentCache.MemoryLimit {
			go me.contentCache.FaultIn(a.Hash)
		}
	}
	rep.Attrs = append(rep.Attrs, a)
	dt := time.Nanoseconds() - start
	me.stats.Log("FsServer.GetAttr", dt)
	return nil
}
