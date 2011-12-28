package cba

import (
	"io"
	"log"
	"net/rpc"
	"os"
	"time"
)

const useNewTransport = false

// Client is a thread-safe interface to fetching over a connection.
type Client struct {
	store  *Store
	client *rpc.Client
}

func (store *Store) NewClient(conn io.ReadWriteCloser) *Client {
	cl := &Client{
		store: store,
	}

	if useNewTransport {
		cl.client = rpc.NewClientWithCodec(NewCbaCodec(conn))
	} else {
		cl.client = rpc.NewClient(conn)
	}
	return cl
}

func (c *Client) Close() {
	c.client.Close()
}

func (c *Store) ServeConn(conn io.ReadWriteCloser) {
	s := Server{c}
	rpcServer := rpc.NewServer()
	rpcServer.Register(&s)
	if useNewTransport {
		rpcServer.ServeCodec(NewCbaCodec(conn))
	} else {
		rpcServer.ServeConn(conn)
	}
	conn.Close()
}

type Server struct {
	store *Store
}

func (s *Server) ServeChunk(req *Request, rep *Response) (err error) {
	start := time.Now()
	err = s.store.ServeChunk(req, rep)
	s.store.addThroughput(0, int64(len(rep.Chunk)))
	dt := time.Now().Sub(start)
	s.store.timings.Log("ContentStore.ServeChunk", dt)
	s.store.timings.LogN("ContentStore.ServeChunkBytes", int64(len(rep.Chunk)), dt)
	return err
}

func (st *Store) ServeChunk(req *Request, rep *Response) (err error) {
	if !st.HasHash(req.Hash) {
		rep.Have = false
		return nil
	}

	rep.Have = true
	if c := st.ContentsIfLoaded(req.Hash); c != nil {
		if req.End > len(c) {
			req.End = len(c)
		}
		rep.Chunk = c[req.Start:req.End]
		rep.Size = len(c)
		return nil
	}

	f, err := os.Open(st.Path(req.Hash))
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
	start := time.Now()
	succ, size, err := c.fetch(want)
	dt := time.Now().Sub(start)
	c.store.timings.Log("ContentStore.Fetch", dt)
	c.store.timings.LogN("ContentStore.FetchBytes", int64(size), dt)
	return succ, err
}

func (c *Client) fetchChunk(req *Request, rep *Response) error {
	start := time.Now()
	err := c.client.Call("Server.ServeChunk", req, rep)
	dt := time.Now().Sub(start)
	c.store.timings.Log("ContentStore.FetchChunk", dt)
	c.store.timings.LogN("ContentStore.FetchChunkBytes", int64(rep.Size), dt)
	return err
}

// TODO - pass size so we alloc smaller chunks.
func (c *Client) fetch(want string) (bool, int, error) {
	chunkSize := 1 << 18
	buf := make([]byte, chunkSize)

	var output *HashWriter
	written := 0

	var saved string
	for {
		req := &Request{
			Hash:  want,
			Start: written,
			End:   written + chunkSize,
		}
		rep := &Response{Chunk: buf}
		err := c.fetchChunk(req, rep)
		if err != nil || !rep.Have {
			return false, 0, err
		}

		// is this a bug in the rpc package?
		content := rep.Chunk[:rep.Size]

		if len(content) < chunkSize && written == 0 {
			saved = c.store.Save(content)
			written = len(content)
			break
		} else if output == nil {
			output = c.store.NewHashWriter()
			defer output.Close()
		}

		n, err := output.Write(content)
		written += n
		if err != nil {
			return false, 0, err
		}
		if len(content) < chunkSize {
			break
		}
	}
	if output != nil {
		output.Close()
		saved = string(output.Sum())
	}
	c.store.addThroughput(int64(written), 0)
	if want != saved {
		log.Fatalf("file corruption: got %x want %x", saved, want)
	}
	return true, written, nil
}
