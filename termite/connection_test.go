package termite

import (
	"fmt"
	"io"
	"os"
	"net"
	"testing"
	"time"
	"rand"
	"github.com/hanwen/go-fuse/fuse"
)

func TestAuthenticate(t *testing.T) {
	secret := []byte("sekr3t")

	// TODO - tiny security hole here.
	port := int(rand.Int31n(60000) + 1024)

	out := make(chan net.Conn)
	go SetupServer(int(port), secret, out)
	time.Sleep(1e9)
	hostname, _ := os.Hostname()
	addr := fmt.Sprintf("%s:%d", hostname, port)
	_, err := DialTypedConnection(addr, RPC_CHANNEL, secret)
	if err != nil {
		t.Fatal("unexpected failure", err)
	}
	<-out

	c, err := DialTypedConnection(addr, RPC_CHANNEL, []byte("foobar"))
	if c != nil {
		t.Error("expect failure")
	}
}

type DummyConn struct {
	*os.File
}

func (me *DummyConn) LocalAddr() net.Addr {
	return &net.UnixAddr{}
}

func (me *DummyConn) RemoteAddr() net.Addr {
	return &net.UnixAddr{}
}

func (me *DummyConn) SetTimeout(nsec int64) os.Error {
	return nil
}

func (me *DummyConn) SetReadTimeout(nsec int64) os.Error {
	return nil
}

func (me *DummyConn) SetWriteTimeout(nsec int64) os.Error {
	return nil
}

func TestPendingConnection(t *testing.T) {
	a1, b1, _ := fuse.Socketpair("unix")
	a2, b2, _ := fuse.Socketpair("unix")

	conn1 := &DummyConn{b1}
	conn2 := &DummyConn{b2}

	id1 := "pqrxyzab"
	id2 := "pqrxy111"

	io.WriteString(a1, id1)
	io.WriteString(a2, id2)

	pc := NewPendingConnections()

	out := make(chan net.Conn)
	go func() {
		out <- pc.WaitConnection(id1)
	}()
	go pc.Accept(conn1)
	go pc.Accept(conn2)
	io.WriteString(a1, "hello")

	other1 := <-out
	b := make([]byte, 100)
	n, err := other1.Read(b)
	if string(b[:n]) != "hello" || err != nil {
		t.Error("unexpected", string(b[:n]), err)
	}

	other2 := pc.WaitConnection(id2)
	io.WriteString(a2, "hallo")

	b = make([]byte, 100)
	n, err = other2.Read(b)
	if string(b[:n]) != "hallo" || err != nil {
		t.Error("unexpected", string(b[:n]), err)
	}
}
