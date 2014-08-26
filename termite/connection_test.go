package termite

import (
	"crypto/rand"
	"crypto/rsa"
	"fmt"
	"io"
	"net"
	"os"
	"testing"

	"code.google.com/p/go.crypto/ssh"
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

			if err := authenticate(c, secret); err != nil {
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

	m, _ := dialer.Dial(addr)

	c, _ := m.Open(RPC_CHANNEL)
	c.Close()
	if <-ch == nil {
		t.Fatal("unexpected failure")
	}

	dialer = newTCPDialer([]byte("foobar"))
	m, _ = dialer.Dial(addr)
	c, _ = m.Open(RPC_CHANNEL)
	if c != nil {
		c.Close()
	}
	if <-ch != nil {
		t.Fatal("unexpected success")
	}
	l.Close()
}

func testDialerMux(t *testing.T, dialer connDialer, listener connListener) {
	found := make(chan bool, 10)
	go func() {
		for c := range listener.RPCChan() {
			go func() {
				var b [HEADER_LEN]byte
				n, _ := c.Read(b[:])
				conn := listener.Accept(string(b[:n]))
				found <- conn != nil
			}()
		}
	}()

	mux, err := dialer.Dial(listener.Addr().String())
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}

	id := ConnectionId()
	if ch, err := mux.Open(RPC_CHANNEL); err != nil {
		t.Fatalf("Open(%q): %v", RPC_CHANNEL, err)
	} else {
		ch.Write([]byte(id))
	}

	if ch, err := mux.Open(id); err != nil {
		t.Fatalf("Open(%q): %v", id, err)
	} else {
		ch.Close()
	}

	mux.Close()

	if !<-found {
		t.Fatal("Did not accept requested channel.")
	}
}

func TestTCPMux(t *testing.T) {
	secret := make([]byte, 20)
	dialer := newTCPDialer(secret)
	l, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatal("net.Listen", err)
	}

	listener := newTCPListener(l, secret)

	testDialerMux(t, dialer, listener)
}

func TestSSHMux(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 512)
	if err != nil {
		t.Fatal("GenerateKey", err)
	}

	l, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatal("net.Listen", err)
	}

	id, err := ssh.NewSignerFromKey(key)
	if err != nil {
		t.Fatal("NewSignerFromKey(%T)", key, err)
	}

	listener := newSSHListener(l, id)
	dialer := newSSHDialer(id)

	testDialerMux(t, dialer, listener)
}
