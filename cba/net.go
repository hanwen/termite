package cba

import (
	"io"
	"net/rpc"
	"os"
	"time"
)

var defaultServeSize = 64 * (1 << 10)


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
	s.store.AddTiming("ServeChunk", len(rep.Chunk), dt)
	return err
}

func (st *Store) TryServeChunkFromMemory(req *Request, rep *Response) bool {
	c := st.ContentsIfLoaded(req.Hash)

	if c == nil {
		return false
	}
	rep.Chunk = c[req.Start:]
	rep.Size = len(rep.Chunk)
	rep.Last = true
	rep.Have = true
	return false
}

func (st *Store) ServeChunk(req *Request, rep *Response) (err error) {
	if !st.HasHash(req.Hash) {
		rep.Have = false
		return nil
	}

	if st.TryServeChunkFromMemory(req, rep) {
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
