package termite

import (
	"fmt"
	"io"
	"net"
	"os"
	"testing"
)

func TestAuthenticate(t *testing.T) {
	secret := RandomBytes(20)

	l, _ := net.Listen("tcp", ":0")
	go func() {
		for {
			c, err := l.Accept()
			if err != nil {
				break
			}

			if err := Authenticate(c, secret); err != nil {
				c.Close()
			}
		}
	}()

	hostname, _ := os.Hostname()
	addr := fmt.Sprintf("%s:%d", hostname, l.Addr().(*net.TCPAddr).Port)
	c, err := DialTypedConnection(addr, RPC_CHANNEL, secret)
	if err != nil {
		t.Fatal("unexpected failure", err)
	}
	c.Close()

	c, err = DialTypedConnection(addr, RPC_CHANNEL, []byte("foobar"))
	if c != nil {
		t.Error("expect failure")
	}
	l.Close()
}

func TestPendingConnection(t *testing.T) {
	a1, conn1, _ := netPair()
	a2, conn2, _ := netPair()
	defer a1.Close()
	defer a2.Close()
	defer conn1.Close()
	defer conn2.Close()

	id1 := ConnectionId()
	id2 := ConnectionId()

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
