package termite

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"rpc"
)

// TODO - should have an interface that opens a network connection and
// streams the whole file directly, to avoid choppy RPCs.

type ContentRequest struct {
	// Either hash or FileInfo must be non-nil
	*os.FileInfo
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
	Cache *ContentCache
}

func (me *ContentServer) FileContent(req *ContentRequest, rep *ContentResponse) os.Error {
	path := ""
	if req.Hash != nil {
		log.Printf("FileContent %x", req.Hash)
		path = HashPath(me.Cache.dir, req.Hash)
	} else {
		// TODO - cross check size & timestamp.
		log.Printf("FileContent %s", req.FileInfo.Name)
		path = req.FileInfo.Name
	}
	
	f, err := os.Open(path)
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
func FetchByHash(client *rpc.Client, rpcName string, size int64, hash []byte) ([]byte, os.Error) {
	req := ContentRequest{
			Hash:  hash,
	}
	return FetchFromContentServer(client, rpcName, size, req)
}

func FetchFromContentServer(client *rpc.Client, rpcName string, size int64, req ContentRequest) ([]byte, os.Error) {
	buf := bytes.NewBuffer(make([]byte, 0, size))
	chunkSize := 1 << 18
	for {
		req.Start = buf.Len()
		req.End = buf.Len() + chunkSize

		rep := &ContentResponse{}
		err := client.Call(rpcName, &req, rep)
		if err != nil {
			log.Println("FileContent error:", err)
			return nil, err
		}

		buf.Write(rep.Chunk)
		if len(rep.Chunk) < chunkSize {
			break
		}
	}

	if buf.Len() < int(size) {
		return nil, os.NewError(
			fmt.Sprintf("Size mismatch %d != %d", buf.Len(), size))
	}
	return buf.Bytes(), nil
}

// FetchHash issues a FileContent RPC to read an entire file.
func FetchByPath(client *rpc.Client, rpcName string, fi os.FileInfo) ([]byte, os.Error) {
	req := ContentRequest{
		FileInfo: &fi,
	}
	return FetchFromContentServer(client, rpcName, fi.Size, req)
}
