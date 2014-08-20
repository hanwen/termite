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
