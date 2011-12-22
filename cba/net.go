package cba

import (
	"encoding/binary"
	"fmt"
	"os"
	"io"
	"sync"
)

var order = binary.BigEndian

// Client is a thread-safe interface to fetching over a connection.
type Client struct {
	cache *ContentCache
	conn  io.ReadWriteCloser
	mu sync.Mutex
}

func (cache *ContentCache) NewClient(conn io.ReadWriteCloser) *Client {
	return &Client{
		conn: conn,
		cache: cache,
	}
}

func (c *Client) Close() {
	c.conn.Close()
}

func (c *Client) Fetch(want string) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	
	// For some reason, we can't binary.Write string.
	wantBytes := []byte(want)
	
	err := binary.Write(c.conn, order, wantBytes)
	if err != nil {
		return false, err
	}

	var have byte
	if err := binary.Read(c.conn, order, &have); err != nil || have == 0 {
		return false, err
	}
	
	var size int64
	if err := binary.Read(c.conn, order, &size); err != nil {
		return false, err
	}
	
	// TODO - SaveStream should return error
	got := c.cache.SaveStream(c.conn, size)
	if got == "" {
		return false, fmt.Errorf("SaveStream returned empty")
	}

	if got != want {
		return false, fmt.Errorf("Mismatch got %x want %x", got, want)
	}

	return true, nil
}

func (c *ContentCache) ServeConn(conn io.ReadWriteCloser) (err error) {
	for {
		err = c.ServeOne(conn)
		if err != nil {
			break
		}
	}
	return err
}

func (c *ContentCache) ServeOne(conn io.ReadWriteCloser) (err error) {
	reqBytes := make([]byte, c.Options.Hash.Size())
	if err := binary.Read(conn, order, reqBytes); err != nil {
		return err
	}
	request := string(reqBytes)

	has := byte(0)
	if c.HasHash(string(request)) {
		has = 1
	}

	if err := binary.Write(conn, order, has); err != nil {
		return err
	}
		
	if has == 0 {
		return nil
	}

	if c := c.ContentsIfLoaded(request); c != nil {
		s := int64(len(c))
		if err := binary.Write(conn, order, s); err != nil {
			return err
		}
		return binary.Write(conn, order, c)
	}

	p := c.Path(request)
	f, err := os.Open(p)
	if err != nil {
		return err
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		return err
	}
	s := int64(fi.Size())
	if err := binary.Write(conn, order, s); err != nil {
		return err
	}

	// TODO - use splice here.
	_, err = io.CopyN(conn, f, s)
	return err
}
