package termite

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"rpc"
)

type ContentRequest struct {
	Hash       []byte
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

func (me *ContentServer) FileContent(req *ContentRequest, rep *ContentResponse) os.Error {
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

// FetchHash issues a FileContent RPC to read an entire file.
func FetchFromContentServer(client *rpc.Client, rpcName string, size int64, hash []byte) []byte {
	chunkSize := 1 << 18

	buf := bytes.NewBuffer(make([]byte, 0, size))
	for {
		req := &ContentRequest{
			Hash:  hash,
			Start: buf.Len(),
			End:   buf.Len() + chunkSize,
		}

		rep := &ContentResponse{}
		err := client.Call(rpcName, req, rep)
		if err != nil && err != os.EOF {
			log.Println("FileContent error:", err)
			break
		}

		buf.Write(rep.Chunk)
		if len(rep.Chunk) < chunkSize {
			break
		}
	}

	if buf.Len() < int(size) {
		log.Fatal("Size mismatch", buf.Len(), size)
	}
	return buf.Bytes()
}
