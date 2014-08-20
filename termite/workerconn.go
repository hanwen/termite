package termite

import (
	"io"
	"net"
)

// connDialer dials connections that have IDs beyond address.
type connDialer interface {
	Open(addr string, id string) (io.ReadWriteCloser, error)
}

// connListener accepts connections that have string IDs
type connListener interface {
	Addr() net.Addr
	Accept(id string) io.ReadWriteCloser
	Close() error
	Wait()
}
