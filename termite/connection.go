package termite

import (
	"bytes"
	"fmt"
	"log"
	"net"
	"os"
	"rand"
	"crypto/hmac"
)

const challengeLength = 20

// TODO - should multiplex single connection.  Will let us pierce a
// firewall one-way.

func Authenticate(conn net.Conn, secret []byte) os.Error {
	challenge := make([]byte, 0)
	for i := 0; i < challengeLength; i++ {
		challenge = append(challenge, byte(rand.Int31n(256)))
	}

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
