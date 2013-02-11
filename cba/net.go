package cba

import (
	"io"
	"net/rpc"
	"os"
	"time"
)

var defaultServeSize = 64 * (1 << 10)

func (c *Store) ServeConn(conn io.ReadWriteCloser) {
	s := c.newServer()
	rpcServer := rpc.NewServer()
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
	s.store.AddTiming("ServeChunk", len(rep.Chunk), dt)
	return err
}

func (st *Store) ServeChunk(req *Request, rep *Response) (err error) {
	if !st.Has(req.Hash) {
		rep.Have = false
		return nil
	}

	rep.Have = true

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
