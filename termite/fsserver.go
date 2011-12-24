package termite

import (
	"github.com/hanwen/termite/attr"
	"github.com/hanwen/termite/cba"
	"github.com/hanwen/termite/stats"
	"log"
	"time"
)

type FsServer struct {
	content *cba.Store
	attributes   *attr.AttributeCache
	stats        *stats.TimerStats
}

func NewFsServer(a *attr.AttributeCache, cache *cba.Store) *FsServer {
	me := &FsServer{
		content: cache,
		attributes:   a,
		stats:        stats.NewTimerStats(),
	}

	return me
}

func (me *FsServer) GetAttr(req *AttrRequest, rep *AttrResponse) error {
	start := time.Now()
	log.Printf("GetAttr %s req %q", req.Origin, req.Name)
	if req.Name != "" && req.Name[0] == '/' {
		panic("leading /")
	}

	a := me.attributes.GetDir(req.Name)
	if a.Hash != "" {
		log.Printf("GetAttr %v", a)
		if a.Size < uint64(me.content.Options.MemMaxSize) {
			go me.content.FaultIn(a.Hash)
		}
	}
	rep.Attrs = append(rep.Attrs, a)
	dt := time.Now().Sub(start)
	me.stats.Log("FsServer.GetAttr", dt)
	return nil
}
