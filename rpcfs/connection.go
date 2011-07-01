package rpcfs

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

func AuthenticateClient(conn net.Conn, secret []byte) os.Error {
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
	response := make([]byte, len(expected))
	n, err := conn.Read(response)
	if err != nil {
		return err
	}
	response = response[:n]

	if bytes.Compare(response, expected) != 0 {
		log.Println("Authentication failure from", conn.RemoteAddr())
		return os.NewError("Mismatch in response")
	}
	conn.Write([]byte("OK"))
	return nil
}

func AuthenticateServer(conn net.Conn, secret []byte) os.Error {
	challenge := make([]byte, challengeLength)
	n, err := conn.Read(challenge)
	if err != nil {
		return err
	}
	challenge = challenge[:n]

	h := hmac.NewSHA1(secret)
	h.Write(challenge)
	_, err = conn.Write(h.Sum())
	if err != nil {
		return err
	}

	expectAck := []byte("OK")
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

		err = AuthenticateClient(conn, secret)
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

	err = AuthenticateServer(conn, secret)
	if err != nil {
		return nil, err
	}

	return conn, nil
}
