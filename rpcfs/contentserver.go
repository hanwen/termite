package rpcfs

import (
	"fmt"
	"os"
)

type ContentRequest struct {
	Hash []byte
	Start, End int
}

func (me *ContentRequest) String() string {
	return fmt.Sprintf("%x [%d, %d]", me.Hash, me.Start, me.End)
}

type ContentResponse struct {
	Chunk []byte
}

// Content server exposes an md5 keyed content store for RPC.
type ContentServer struct {
	Cache *DiskFileCache
}

func (me *ContentServer) FileContent(req *ContentRequest, rep *ContentResponse) (os.Error) {
	f, err := os.Open(HashPath(me.Cache.dir, req.Hash))
	if err != nil {
		return err
	}
	defer f.Close()

	rep.Chunk = make([]byte, req.End-req.Start)
	n, err := f.ReadAt(rep.Chunk, int64(req.Start))
	rep.Chunk = rep.Chunk[:n]

	if err == os.EOF {
		err = nil
	}
	return err
}

