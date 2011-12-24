package cba

import (
	"encoding/binary"
	"log"
	"os"
	"io"
	"net/rpc"
)

var order = binary.BigEndian

// Client is a thread-safe interface to fetching over a connection.
type Client struct {
	cache *ContentCache
	client *rpc.Client
}

func (cache *ContentCache) NewClient(conn io.ReadWriteCloser) *Client {
	return &Client{
	cache: cache,
	client: rpc.NewClient(conn),
	}
}

func (c *Client) Close() {
	c.client.Close()
}


func (c *ContentCache) ServeConn(conn io.ReadWriteCloser) {
	s := Server{c}
	rpcServer := rpc.NewServer()
	rpcServer.Register(&s)
	rpcServer.ServeConn(conn)
	conn.Close()
}

type Server struct {
	cache *ContentCache
}

func (s *Server) ServeChunk(req *Request, rep *Response) (err error) {
	e := s.cache.ServeChunk(req, rep)
	return e
}

func (me *ContentCache) ServeChunk(req *Request, rep *Response) (err error) {
	if !me.HasHash(req.Hash) {
		rep.Have = false
		return nil
	}

	rep.Have = true
	if c := me.ContentsIfLoaded(req.Hash); c != nil {
		if req.End > len(c) {
			req.End = len(c)
		}
		rep.Chunk = c[req.Start:req.End]
		rep.Size = len(c)
		return nil
	}

	f, err := os.Open(me.Path(req.Hash))
	if err != nil {
		return err
	}
	defer f.Close()

	rep.Chunk = make([]byte, req.End-req.Start)
	n, err := f.ReadAt(rep.Chunk, int64(req.Start))
	rep.Chunk = rep.Chunk[:n]
	rep.Size = n
	
	if err == io.EOF {
		err = nil
	}
	return err
}
 
func (c *Client) Fetch(want string) (bool, error) {
	chunkSize := 1 << 18
	buf := make([]byte, chunkSize)

	var output *HashWriter
	written := 0

	var saved string
	for {
		
		req := &Request{
		Hash:  want,
		Start: written,
		End:   written+chunkSize,
		}
		rep := &Response{Chunk: buf}
		err := c.client.Call("Server.ServeChunk", req, rep)
		if err != nil || !rep.Have {
			return false, err
		}

		// is this a bug in the rpc package?
		content := rep.Chunk[:rep.Size]

		if len(content) < chunkSize && written == 0 {
			saved = c.cache.Save(content)
			break
		} else if output == nil {
			output = c.cache.NewHashWriter()
			defer output.Close()
		}

		n, err := output.Write(content)
		written += n
		if err != nil {
			return false, err
		}
		if len(content) < chunkSize {
			break
		}
	}
	if output != nil {
		output.Close()
		saved = string(output.Sum())
	}
	if want != saved {
		log.Fatalf("file corruption: got %x want %x", saved, want)
	}
	return true, nil
}


