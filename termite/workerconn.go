package termite

import (
	"io"
	"net"
	"sync"
)

// connDialer dials connections that have IDs beyond address.
type connDialer interface {
	Dial(addr string) (connMuxer, error)
}

// connMuxer opens multiple named streams over a connection
type connMuxer interface {
	Open(id string) (io.ReadWriteCloser, error)
	Close() error
}

// connListener accepts connections that have string IDs.
type connListener interface {
	Addr() net.Addr
	Close() error
	Pending() *pendingConns
}

type pendingConns struct {
	rpcChans chan io.ReadWriteCloser
	conns    map[string]io.ReadWriteCloser
	cond     sync.Cond
}

func newPendingConns() *pendingConns {
	p := &pendingConns{
		conns:    map[string]io.ReadWriteCloser{},
		rpcChans: make(chan io.ReadWriteCloser, 1),
	}
	p.cond.L = new(sync.Mutex)
	return p
}

func (p *pendingConns) rpcChan() <-chan io.ReadWriteCloser {
	return p.rpcChans
}

func (p *pendingConns) fail() {
	p.cond.L.Lock()
	defer p.cond.L.Unlock()
	p.conns = nil
	p.cond.Broadcast()
}

func (p *pendingConns) wait() {
	p.cond.L.Lock()
	defer p.cond.L.Unlock()
	for p.conns != nil {
		p.cond.Wait()
	}
}

func (p *pendingConns) add(key string, conn io.ReadWriteCloser) {
	if key == RPC_CHANNEL {
		p.rpcChans <- conn
		return
	}

	p.cond.L.Lock()
	defer p.cond.L.Unlock()
	if p.conns == nil {
		panic("shut down")
	}
	if p.conns[key] != nil {
		panic("collision")
	}
	p.conns[key] = conn
	p.cond.Broadcast()
}

func (p *pendingConns) accept(key string) io.ReadWriteCloser {
	p.cond.L.Lock()
	defer p.cond.L.Unlock()
	for p.conns != nil && p.conns[key] == nil {
		p.cond.Wait()
	}
	if p.conns == nil {
		return nil
	}

	ch := p.conns[key]
	delete(p.conns, key)
	return ch
}
