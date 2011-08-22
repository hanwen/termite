package termite

import (
	"fmt"
	"log"
	"os"
	"rpc"
)

// TODO - should have an interface that opens a network connection and
// streams the whole file directly, to avoid choppy RPCs?

type ContentRequest struct {
	Hash       string
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
	f, err := os.Open(me.Cache.Path(req.Hash))
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

// FetchHash issues a FileContent RPC to read an entire file, and store into ContentCache.
//
// TODO - open a connection for this instead.
func FetchBetweenContentServers(client *rpc.Client, rpcName string, size int64, hash string,
dest *ContentCache) os.Error {
	chunkSize := 1 << 18

	output := dest.NewHashWriter()
	written := 0
	for {
		req := &ContentRequest{
			Hash:  hash,
			Start: written,
			End:   written + chunkSize,
		}

		rep := &ContentResponse{}
		err := client.Call(rpcName, req, rep)
		if err != nil {
			log.Println("FileContent error:", err)
			return err
		}

		n, err := output.Write(rep.Chunk)
		written += n
		if err != nil {
			return err
		}
		if len(rep.Chunk) < chunkSize {
			break
		}
	}

	if written < int(size) {
		return os.NewError(
			fmt.Sprintf("Size mismatch %d != %d", written, size))
	}

	output.Close()
	saved := string(output.hasher.Sum())
	if saved != hash {
		log.Fatalf("Corruption: savedHash %x != requested hash %x.", saved, hash)
	}
	return nil
}
