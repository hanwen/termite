package termite

import (
	"bytes"
	"fmt"
	"io"
	"net"

	"code.google.com/p/go.crypto/ssh"
)

type sshDialer struct {
	identity ssh.Signer
}

func newSSHDialer(id ssh.Signer) connDialer {
	return &sshDialer{id}
}

func (d *sshDialer) checkHost(hostname string, remote net.Addr, key ssh.PublicKey) error {
	if bytes.Equal(key.Marshal(), d.identity.PublicKey().Marshal()) {
		return nil
	}
	return fmt.Errorf("key mismatch")
}

func (d *sshDialer) Dial(addr string) (connMuxer, error) {
	conf := ssh.ClientConfig{
		User:            "termite",
		Auth:            []ssh.AuthMethod{ssh.PublicKeys(d.identity)},
		HostKeyCallback: d.checkHost,
	}

	c, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}

	defer func() {
		if c != nil {
			c.Close()
		}
	}()
	conn, chans, reqs, err := ssh.NewClientConn(c, addr, &conf)
	if err != nil {
		return nil, err
	}
	go ssh.DiscardRequests(reqs)
	go func() {
		for c := range chans {
			go c.Reject(ssh.Prohibited, "")
		}
	}()

	c = nil
	return &sshMuxer{conn}, nil
}

type sshMuxer struct {
	conn ssh.Conn
}

func (m *sshMuxer) Close() error {
	return m.conn.Close()
}

func (m *sshMuxer) Open(id string) (io.ReadWriteCloser, error) {
	channel, reqs, err := m.conn.OpenChannel(id, nil)
	if err != nil {
		return nil, err
	}
	go ssh.DiscardRequests(reqs)

	return channel, nil
}

type sshListener struct {
	id       ssh.Signer
	listener net.Listener
	pending  *pendingConns
	rpcChans chan io.ReadWriteCloser
}

func (l *sshListener) Addr() net.Addr {
	return l.listener.Addr()
}

func (l *sshListener) Accept(id string) io.ReadWriteCloser {
	return l.pending.accept(id)
}

func (l *sshListener) Close() error {
	err := l.listener.Close()
	l.pending.fail()
	return err
}

func (l *sshListener) RPCChan() <-chan io.ReadWriteCloser {
	return l.rpcChans
}

func (l *sshListener) checkLogin(conn ssh.ConnMetadata, key ssh.PublicKey) (*ssh.Permissions, error) {
	if bytes.Equal(key.Marshal(), l.id.PublicKey().Marshal()) && conn.User() == "termite" {
		return nil, nil
	}

	return nil, fmt.Errorf("denied")
}

func newSSHListener(listener net.Listener, id ssh.Signer) connListener {
	l := sshListener{
		id:       id,
		pending:  newPendingConns(),
		listener: listener,
		rpcChans: make(chan io.ReadWriteCloser, 1),
	}
	go l.loop()

	return &l
}

func (l *sshListener) loop() {
	for {
		conn, err := l.listener.Accept()
		if err != nil {
			break
		}

		go l.handle(conn)
	}
	l.pending.fail()
}

func (l *sshListener) handle(c net.Conn) error {
	conf := ssh.ServerConfig{
		PublicKeyCallback: l.checkLogin,
	}
	conf.AddHostKey(l.id)
	_, chans, reqs, err := ssh.NewServerConn(c, &conf)
	if err != nil {
		return err
	}

	go ssh.DiscardRequests(reqs)
	for newCh := range chans {
		id := newCh.ChannelType()
		if len(id) != len(RPC_CHANNEL) {
			newCh.Reject(ssh.Prohibited, "wrong ID length")
		}

		ch, reqs, err := newCh.Accept()
		if err != nil {
			continue
		}
		go ssh.DiscardRequests(reqs)

		if id == RPC_CHANNEL {
			l.rpcChans <- ch
		} else {
			l.pending.add(id, ch)
		}
	}
	return nil
}
