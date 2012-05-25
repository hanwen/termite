package cba

import (
	"github.com/hanwen/go-fuse/splice"
	"log"
	"os"
	"sync"
	"time"
)

type ServeSplice struct {
	pair *splice.Pair
	off  int64
	size int
	last bool
}

// Reads given file in chunkSize blocks, sending each block to the
// output channel.
func fill(f *os.File, chunkSize int, out chan ServeSplice) {
	defer f.Close()
	var off int64
	for {
		p, err := splice.Get()
		if err != nil {
			log.Panicf("splice.Get: %v", err)
		}
		p.Grow(chunkSize)

		n, err := p.LoadFrom(f.Fd(), chunkSize)
		if err != nil {
			log.Panicf("LoadFrom %v", err)
		}

		out <- ServeSplice{
			off:  off,
			size: n,
			pair: p,
			last: n < chunkSize,
		}
		off += int64(n)
		if n < chunkSize {
			break
		}
	}
	close(out)
}

func newSpliceSequence(name string) (chan ServeSplice, error) {
	f, err := os.Open(name)
	if err != nil {
		return nil, err
	}

	p := make(chan ServeSplice)
	go fill(f, splice.DefaultPipeSize, p)
	return p, nil
}

type serverKey struct {
	hash string
	off  int64
}

// spliceServer stores that are in progress of being served.
type spliceServer struct {
	store   *Store
	mu      sync.Mutex
	pending map[serverKey][]chan ServeSplice
}

func newSpliceServer(store *Store) *spliceServer {
	return &spliceServer{
		store:   store,
		pending: make(map[serverKey][]chan ServeSplice),
	}
}

func (s *spliceServer) ServeChunk(req *Request, rep *Response) (err error) {
	start := time.Now()
	err = s.serveChunk(req, rep)
	s.store.addThroughput(0, int64(len(rep.Chunk)))
	dt := time.Now().Sub(start)
	s.store.AddTiming("ServeChunk", len(rep.Chunk), dt)
	return err
}

// Get the next splice, read it into the response.
func (s *spliceServer) serveChunk(req *Request, rep *Response) (err error) {
	if s.store.TryServeChunkFromMemory(req, rep) {
		return nil
	}

	if req.Start == 0 {
		err := s.prepareServe(req.Hash)
		if err != nil {
			rep.Have = false
			return nil
		}
	}
	rep.Have = true

	spl := s.serve(req.Hash, int64(req.Start))
	if spl == nil {
		return s.store.ServeChunk(req, rep)
	}
	defer splice.Done(spl.pair)

	data := make([]byte, spl.size)
	n, err := spl.pair.Read(data)
	if err != nil {
		return s.store.ServeChunk(req, rep)
	}
	rep.Chunk = data[:n]
	rep.Size = n
	rep.Last = spl.last
	return nil
}

func (s *spliceServer) insert(h string, off int64, ch chan ServeSplice) {
	s.mu.Lock()
	defer s.mu.Unlock()

	k := serverKey{h, off}
	old := s.pending[k]
	s.pending[k] = append(old, ch)
}

func (s *spliceServer) remove(h string, off int64) (out chan ServeSplice) {
	s.mu.Lock()
	defer s.mu.Unlock()

	k := serverKey{h, off}
	old := s.pending[k]
	if len(old) > 0 {
		out = old[len(old)-1]
		s.pending[k] = old[:len(old)-1]
	}
	return
}

func (s *spliceServer) prepareServe(h string) error {
	seq, err := newSpliceSequence(s.store.Path(h))
	if err != nil {
		return err
	}

	s.insert(h, 0, seq)
	return nil
}

func (s *spliceServer) serve(h string, off int64) *ServeSplice {
	seq := s.remove(h, off)
	if seq == nil {
		return nil
	}
	data := <-seq

	if !data.last {
		s.insert(h, off+int64(data.size), seq)
	}

	return &data
}

func (s *spliceServer) Close() {
	chans := []chan ServeSplice{}
	s.mu.Lock()
	for _, p := range s.pending {
		chans = append(chans, p...)
	}
	s.pending = map[serverKey][]chan ServeSplice{}
	s.mu.Unlock()

	for _, c := range chans {
		for r := range c {
			splice.Done(r.pair)
		}
	}
}
