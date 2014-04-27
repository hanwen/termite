package termite

import (
	"fmt"
	"io"
	//	"log"
	"net"
	"os"
	"testing"
)

func TestAuthenticate(t *testing.T) {
	secret := RandomBytes(20)

	l, _ := net.Listen("tcp", ":0")
	ch := make(chan io.ReadWriteCloser, 1)
	go func() {
		for {
			c, err := l.Accept()
			if err != nil {
				break
			}

			if err := Authenticate(c, secret); err != nil {
				c.Close()
				ch <- nil
			} else {
				ch <- c
			}
		}
	}()

	hostname, _ := os.Hostname()
	addr := fmt.Sprintf("%s:%d", hostname, l.Addr().(*net.TCPAddr).Port)
	dialer := newTCPDialer(secret)

	c, _ := dialer.Open(addr, RPC_CHANNEL)
	c.Close()
	if <-ch == nil {
		t.Fatal("unexpected failure")
	}

	dialer = newTCPDialer([]byte("foobar"))
	c, _ = dialer.Open(addr, RPC_CHANNEL)
	if c != nil {
		c.Close()
	}
	if <-ch != nil {
		t.Fatal("unexpected success")
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
