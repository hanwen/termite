package termite

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha1"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
)

const challengeLength = 20

type tcpDialer struct {
	secret []byte
}

type tcpMux struct {
	dial *tcpDialer
	addr string
}

// newTCPDialer returns a connDialer that uses plaintext TCP/IP
// connections, and HMAC-SHA1 for authentication. It should not
// be used in hostile environments.
func newTCPDialer(secret []byte) connDialer {
	return &tcpDialer{secret}
}

func (c *tcpDialer) Dial(addr string) (connMuxer, error) {
	return &tcpMux{c, addr}, nil
}

func (m *tcpMux) Close() error {
	return nil
}

func (m *tcpMux) Open(id string) (io.ReadWriteCloser, error) {
	if len(id) != HEADER_LEN {
		return nil, fmt.Errorf("len(%q) != %d", id, HEADER_LEN)
	}

	conn, err := net.Dial("tcp", m.addr)
	if err != nil {
		return nil, err
	}

	if err := authenticate(conn, m.dial.secret); err != nil {
		return nil, err
	}

	if _, err := conn.Write([]byte(id)); err != nil {
		return nil, err
	}
	return conn, nil
}

type tcpListener struct {
	net.Listener
	incoming chan io.ReadWriteCloser
	pending  *pendingConns
	secret   []byte
}

// newTCPListener returns a connListener that uses plaintext TCP/IP
// connections, and HMAC-SHA1 for authentication. It should not be
// used in hostile environments. RPC connections (which use a special
// connection ID) are posted to the given input channel. If the secret
// is nil, the authentication step is skipped, so it can be used for
// Unix domain sockets too.
func newTCPListener(l net.Listener, secret []byte) connListener {
	tl := &tcpListener{
		Listener: l,
		incoming: make(chan io.ReadWriteCloser, 1),
		pending:  newPendingConns(),
		secret:   secret,
	}
	go tl.loop()
	return tl
}

func (l *tcpListener) Pending() *pendingConns {
	return l.pending
}

func (l *tcpListener) loop() {
	for {
		conn, err := l.Listener.Accept()
		if err != nil {
			break
		}

		go l.handleConn(conn)
	}

	l.pending.fail()
}

func (l *tcpListener) handleConn(c net.Conn) {
	if len(l.secret) > 0 {
		if err := authenticate(c, l.secret); err != nil {
			log.Println("authenticate", err)
			c.Close()
			return
		}
	}

	var h [HEADER_LEN]byte
	if _, err := io.ReadFull(c, h[:]); err != nil {
		return
	}

	chType := string(h[:])
	l.pending.add(chType, c)
}

func sign(conn net.Conn, challenge []byte, secret []byte, local bool) []byte {
	h := hmac.New(sha1.New, secret)
	h.Write(challenge)
	l := conn.LocalAddr()
	r := conn.RemoteAddr()
	connSignature := ""
	if local {
		connSignature = fmt.Sprintf("%v-%v", l, r)
	} else {
		connSignature = fmt.Sprintf("%v-%v", r, l)
	}
	h.Write([]byte(connSignature))
	return h.Sum(nil)
}

// Symmetrical authentication using HMAC-SHA1.
//
// To authenticate, we do  the following:
//
// * Receive 20-byte random challenge
// * Using the secret, sign (challenge + remote address + local address)
// * Return the signature
func authenticate(conn net.Conn, secret []byte) error {
	challenge := RandomBytes(challengeLength)

	_, err := conn.Write(challenge)
	if err != nil {
		return err
	}
	expected := sign(conn, challenge, secret, true)

	remoteChallenge := make([]byte, challengeLength)
	n, err := conn.Read(remoteChallenge)
	if err != nil {
		return err
	}
	remoteChallenge = remoteChallenge[:n]
	_, err = conn.Write(sign(conn, remoteChallenge, secret, false))

	response := make([]byte, len(expected))
	n, err = conn.Read(response)
	if err != nil {
		return err
	}
	response = response[:n]

	if bytes.Compare(response, expected) != 0 {
		log.Println("Authentication failure from", conn.RemoteAddr())
		conn.Close()
		return errors.New("Mismatch in response")
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
		return errors.New("Missing ack reply")
	}

	return nil
}
