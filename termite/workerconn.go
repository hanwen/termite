package termite

import (
	"fmt"
	"io"
	"log"
	"net"
	"sync"
)

type connDialer interface {
	Open(addr string, id string) (io.ReadWriteCloser, error)
}

type connListener interface {
	Addr() net.Addr
	Accept(id string) io.ReadWriteCloser
	Close() error
	Wait()
}

type tcpDialer struct {
	secret []byte
}

func newTCPDialer(secret []byte) connDialer {
	return &tcpDialer{secret}
}

func (c *tcpDialer) Open(addr string, id string) (io.ReadWriteCloser, error) {
	if len(id) != HEADER_LEN {
		return nil, fmt.Errorf("len(%q) != %d", id, HEADER_LEN)
	}

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}

	if err := Authenticate(conn, c.secret); err != nil {
		return nil, err
	}

	if _, err := conn.Write([]byte(id)); err != nil {
		return nil, err
	}
	return conn, nil
}

type pendingConns struct {
	conns map[string]io.ReadWriteCloser
	cond  sync.Cond
}

func newPendingConns() *pendingConns {
	p := &pendingConns{
		conns: map[string]io.ReadWriteCloser{},
	}
	p.cond.L = new(sync.Mutex)
	return p
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
		log.Println("wake")
		p.cond.Wait()
	}
	if p.conns == nil {
		return nil
	}

	ch := p.conns[key]
	delete(p.conns, key)
	return ch
}

type tcpListener struct {
	net.Listener
	incoming chan<- io.ReadWriteCloser
	pending  *pendingConns
	secret   []byte
}

func newTCPListener(l net.Listener, secret []byte, rpcChans chan<- io.ReadWriteCloser) connListener {
	tl := &tcpListener{
		Listener: l,
		incoming: rpcChans,
		pending:  newPendingConns(),
		secret:   secret,
	}
	go tl.loop()
	return tl
}

func (l *tcpListener) loop() {
	for {
		conn, err := l.Listener.Accept()
		if err != nil {
			break
		}

		go l.handleConn(conn)
	}

	l.pending.fail()
}

func (l *tcpListener) Wait() {
	l.pending.wait()
}

func (l *tcpListener) handleConn(c net.Conn) {
	if len(l.secret) > 0 {
		if err := Authenticate(c, l.secret); err != nil {
			log.Println("Authenticate", err)
			c.Close()
			return
		}
	}

	var h [HEADER_LEN]byte
	if _, err := io.ReadFull(c, h[:]); err != nil {
		return
	}

	chType := string(h[:])
	if chType == RPC_CHANNEL {
		l.incoming <- c
	} else {
		l.pending.add(chType, c)
	}
}

func (l *tcpListener) Accept(id string) io.ReadWriteCloser {
	return l.pending.accept(id)
}
