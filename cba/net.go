package cba

import (
	"io"
	"log"
	"net/rpc"
	"os"
	"time"
)

var defaultServeSize = 64 * (1<<10)

// Client is a thread-safe interface to fetching over a connection.
type Client struct {
	store  *Store
	client *rpc.Client
}

func (store *Store) NewClient(conn io.ReadWriteCloser) *Client {
	cl := &Client{
		store: store,
	}

	cl.client = rpc.NewClient(conn)
	return cl
}

func (c *Client) Close() {
	c.client.Close()
}

func (c *Store) ServeConn(conn io.ReadWriteCloser) {
	var s Server
	rpcServer := rpc.NewServer()
	s = newSpliceServer(c)
	rpcServer.RegisterName("Server", s)
	rpcServer.ServeConn(conn)
	conn.Close()
	s.Close()
}

type Server interface {
	ServeChunk(req *Request, rep *Response) (err error)
	Close()
}

// Classic RPC server.
type contentServer struct {
	store *Store
}

func (s *contentServer) ServeChunk(req *Request, rep *Response) (err error) {
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
		rep.Chunk = c[req.Start:]
		rep.Size = len(c)
		rep.Last = true
		return nil
	}

	f, err := os.Open(st.Path(req.Hash))
	if err != nil {
		return err
	}
	defer f.Close()

	sz := defaultServeSize 
	rep.Chunk = make([]byte, sz)
	n, err := f.ReadAt(rep.Chunk, int64(req.Start))
	rep.Chunk = rep.Chunk[:n]
	rep.Size = n
	if err == io.EOF {
		err = nil
		rep.Last = true
	}
	return err
}

func (c *Client) Fetch(want string, size int64) (bool, error) {
	start := time.Now()
	succ, err := c.fetch(want, size)
	dt := time.Now().Sub(start)
	c.store.timings.Log("ContentStore.Fetch", dt)
	c.store.timings.LogN("ContentStore.FetchBytes", size, dt)
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

func (c *Client) fetch(want string, size int64) (bool, error) {
	chunkSize := defaultServeSize
	if int64(chunkSize) > size+1 {
		chunkSize = int(size + 1)
	}

	buf := make([]byte, chunkSize)

	var output *HashWriter
	written := 0

	var saved string
	for {
		req := &Request{
			Hash:  want,
			Start: written,
		}
		rep := &Response{Chunk: buf}
		err := c.fetchChunk(req, rep)
		if err != nil || !rep.Have {
			return false, err
		}

		// is this a bug in the rpc package?
		content := rep.Chunk[:rep.Size]

		if rep.Last && written == 0 {
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
			return false, err
		}
		if rep.Last {
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
	return true, nil
}
