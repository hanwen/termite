package termite

import (
	"io"
	"net"
)

// connDialer dials connections that have IDs beyond address.
type connDialer interface {
	Dial(addr string) (connMuxer, error)
}

type connMuxer interface {
	Open(id string) (io.ReadWriteCloser, error)
}

// connListener accepts connections that have string IDs.
type connListener interface {
	Addr() net.Addr
	Accept(id string) io.ReadWriteCloser

	// RPCChan returns the connection for the primary RPC service.
	RPCChan() <-chan io.ReadWriteCloser
	Close() error
}
