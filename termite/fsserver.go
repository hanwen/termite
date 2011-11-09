package termite

import (
	"log"
	"time"
)

type FsServer struct {
	contentCache *ContentCache
	attr         *AttributeCache
	stats        *TimerStats
}

func NewFsServer(attr *AttributeCache, cache *ContentCache) *FsServer {
	me := &FsServer{
		contentCache: cache,
		attr:         attr,
		stats:        NewTimerStats(),
	}

	return me
}

func (me *FsServer) FileContent(req *ContentRequest, rep *ContentResponse) error {
	start := time.Nanoseconds()
	err := ServeFileContent(me.contentCache, req, rep)
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

	a := me.attr.GetDir(req.Name)
	if a.Hash != "" {
		log.Printf("GetAttr %v", a)
		if a.Size < _MEMORY_LIMIT {
			go me.contentCache.FaultIn(a.Hash)
		}
	}
	rep.Attrs = append(rep.Attrs, a)
	dt := time.Nanoseconds() - start
	me.stats.Log("FsServer.GetAttr", dt)
	return nil
}
