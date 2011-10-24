package termite

import (
	"log"
	"os"
)

type FsServer struct {
	contentCache   *ContentCache
	attr           *AttributeCache
}

func NewFsServer(attr *AttributeCache, cache *ContentCache) *FsServer {
	me := &FsServer{
		contentCache:   cache,
		attr: attr,
	}

	return me
}

func (me *FsServer) FileContent(req *ContentRequest, rep *ContentResponse) os.Error {
	return ServeFileContent(me.contentCache, req, rep)
}

func (me *FsServer) GetAttr(req *AttrRequest, rep *AttrResponse) os.Error {
	log.Printf("GetAttr req %q", req.Name)
	if req.Name != "" && req.Name[0] == '/' {
		panic("leading /")
	}

	a := me.attr.GetDir(req.Name)
	if a.Hash != "" {
		log.Printf("GetAttr %v", a)
	}
	rep.Attrs = append(rep.Attrs, a)
	return nil
}

