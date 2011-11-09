package termite

import (
	"fmt"
	"github.com/hanwen/go-fuse/fuse"
	"io"
	"net"
	"os"
	"rand"
	"testing"
	"time"
)

func TestAuthenticate(t *testing.T) {
	secret := RandomBytes(20)
	port := int(rand.Int31n(60000) + 1024)

	l := AuthenticatedListener(port, secret)
	go func() {
		for {
			_, err := l.Accept()
			if err != nil {
				break
			}
		}
	}()

	time.Sleep(1e9)
	hostname, _ := os.Hostname()
	addr := fmt.Sprintf("%s:%d", hostname, port)
	_, err := DialTypedConnection(addr, RPC_CHANNEL, secret)
	if err != nil {
		t.Fatal("unexpected failure", err)
	}

	c, err := DialTypedConnection(addr, RPC_CHANNEL, []byte("foobar"))
	if c != nil {
		t.Error("expect failure")
	}
	l.Close()
}

type dummyConn struct {
	*os.File
}

func (me *dummyConn) LocalAddr() net.Addr {
	return &net.UnixAddr{}
}

func (me *dummyConn) RemoteAddr() net.Addr {
	return &net.UnixAddr{}
}

func (me *dummyConn) SetTimeout(nsec int64) error {
	return nil
}

func (me *dummyConn) SetReadTimeout(nsec int64) error {
	return nil
}

func (me *dummyConn) SetWriteTimeout(nsec int64) error {
	return nil
}

func TestPendingConnection(t *testing.T) {
	a1, b1, _ := fuse.Socketpair("unix")
	a2, b2, _ := fuse.Socketpair("unix")
	defer a1.Close()
	defer a2.Close()
	defer b1.Close()
	defer b2.Close()

	conn1 := &dummyConn{b1}
	conn2 := &dummyConn{b2}

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
