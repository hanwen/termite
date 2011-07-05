package termite

import (
	"bytes"
	"fmt"
	"log"
	"net"
	"os"
	"rand"
	"crypto/hmac"
	"sync"
	"io"
)

const challengeLength = 20

func RandomBytes(n int) []byte {
	c := make([]byte, 0)
	for i := 0; i < n; i++ {
		c = append(c, byte(rand.Int31n(256)))
	}
	return c
}

func Authenticate(conn net.Conn, secret []byte) os.Error {
	challenge := RandomBytes(challengeLength)

	_, err := conn.Write(challenge)
	if err != nil {
		return err
	}
	h := hmac.NewSHA1(secret)
	_, err = h.Write(challenge)
	expected := h.Sum()

	remoteChallenge := make([]byte, challengeLength)
	n, err := conn.Read(remoteChallenge)
	if err != nil {
		return err
	}
	remoteChallenge = remoteChallenge[:n]
	remoteHash := hmac.NewSHA1(secret)
	remoteHash.Write(remoteChallenge)
	_, err = conn.Write(remoteHash.Sum())

	response := make([]byte, len(expected))
	n, err = conn.Read(response)
	if err != nil {
		return err
	}
	response = response[:n]

	if bytes.Compare(response, expected) != 0 {
		log.Println("Authentication failure from", conn.RemoteAddr())
		conn.Close()
		return os.NewError("Mismatch in response")
	}

	expectAck := []byte("OK")
	conn.Write(expectAck)

	ack := make([]byte, len(expectAck))
	n, err = conn.Read(ack)
	if err != nil {
		return err
	}

	ack = ack[:n]
	if bytes.Compare(expectAck, ack) != 0 {
		fmt.Println(expectAck, ack)
		return os.NewError("Missing ack reply")
	}

	return nil
}

func MyAddress(port int) string {
	host, err := os.Hostname()
	if err != nil {
		log.Fatal("hostname", err)
	}

	return fmt.Sprintf("%s:%d", host, port)
}

func SetupServer(port int, secret []byte, output chan net.Conn) {
	addr := MyAddress(port)
	// TODO - also listen on localhost.
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		panic(err)
	}
	log.Println("Listening to", addr)

	for {
		conn, err := listener.Accept()
		if err != nil {
			panic(err)
		}

		err = Authenticate(conn, secret)
		if err != nil {
			log.Println("Authentication error: ", err)
			conn.Close()
			continue
		}
		output <- conn
	}
}

func SetupClient(addr string, secret []byte) (net.Conn, os.Error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}

	err = Authenticate(conn, secret)
	if err != nil {
		return nil, err
	}

	return conn, nil
}

// ids:
//
const (
	RPC_CHANNEL = "rpc....."
	// Put in 4 random bytes
	STDIN_FMT  = "id..%s"
	HEADER_LEN = 8
)

type PendingConnection struct {
	Id    string
	Ready sync.Cond
	Conn  net.Conn
}

type PendingConnections struct {
	connectionsMutex sync.Mutex
	connections      map[string]*PendingConnection
}

func NewPendingConnections() *PendingConnections {
	return &PendingConnections{
		connections: make(map[string]*PendingConnection),
	}
}

func (me *PendingConnections) newPendingConnection(id string) *PendingConnection {
	p := &PendingConnection{
		Id: id,
	}
	p.Ready.L = &me.connectionsMutex
	return p
}

func (me *PendingConnections) WaitConnection(id string) net.Conn {
	me.connectionsMutex.Lock()
	defer me.connectionsMutex.Unlock()
	p := me.connections[id]
	if p == nil {
		p = me.newPendingConnection(id)
		me.connections[id] = p
	}

	for p.Conn == nil {
		p.Ready.Wait()
	}

	me.connections[id] = nil
	return p.Conn
}

func (me *PendingConnections) Accept(conn net.Conn) os.Error {
	idBytes := make([]byte, HEADER_LEN)
	n, err := conn.Read(idBytes)
	if n != HEADER_LEN || err != nil {
		return err
	}
	id := string(idBytes)

	me.connectionsMutex.Lock()
	defer me.connectionsMutex.Unlock()

	p := me.connections[id]
	if p == nil {
		p = me.newPendingConnection(id)
		me.connections[id] = p
	}

	p.Conn = conn
	p.Ready.Signal()
	return nil
}

func DialTypedConnection(addr string, id string, secret []byte) (net.Conn, os.Error) {
	if len(id) != 8 {
		log.Fatal("id != 8", id, len(id))
	}
	conn, err := SetupClient(addr, secret)
	if err != nil {
		return nil, err
	}
	_, err = io.WriteString(conn, id)
	if err != nil {
		return nil, err
	}
	return conn, nil
}
