package rpcfs

import (
	"fmt"
	"net"
	"testing"
	"time"
	"rand"
)

func TestAuthenticate(t *testing.T) {
	secret := []byte("sekr3t")
	addr := fmt.Sprintf("localhost:%d", rand.Int31n(60000) + 1024)

	out := make(chan net.Conn)
	go SetupServer(addr, secret, out)
	time.Sleep(1e9)
	_, err := SetupClient(addr, secret)
	if err != nil {
		t.Fatal("unexpected failure", err)
	}
	<-out

	c, err := SetupClient(addr, []byte("foobar"))
	if c != nil {
		t.Error("expect failure")
	}

}

