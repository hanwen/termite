package cba
import (
	"io"
	"log"
	"net/rpc"
	"sync"
	"time"
)

// Client is a thread-safe interface to fetching over a connection.
type Client struct {
	store  *Store
	client *rpc.Client

	// Below code is used to make sure we only fetch each hash
	// once in FetchOnce.
	mutex    sync.Mutex
	cond     *sync.Cond
	fetching map[string]bool
}

func (store *Store) NewClient(conn io.ReadWriteCloser) *Client {
	cl := &Client{
		store: store,
		fetching: map[string]bool{},
	}
	cl.cond = sync.NewCond(&cl.mutex)
	cl.client = rpc.NewClient(conn)
	return cl
}

func (c *Client) Close() {
	c.client.Close()
}

// FetchOnce makes sure only one fetch is done, if concurrent fetches
// for the same file happen.
func (c *Client) FetchOnce(want string, size int64) (bool, error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	for !c.store.HasHash(want) && c.fetching[want] {
		c.cond.Wait()
	}
	if c.store.HasHash(want) {
		return true, nil
	}
	c.fetching[want] = true
	c.mutex.Unlock()

	got, err := c.Fetch(want, size)
	c.mutex.Lock()
	delete(c.fetching, want)
	c.cond.Broadcast()
	
	return got, err
}

func (c *Client) Fetch(want string, size int64) (bool, error) {
	start := time.Now()
	succ, err := c.fetch(want, size)
	dt := time.Now().Sub(start)
	c.store.AddTiming("Fetch", int(size), dt)
	return succ, err
}

func (c *Client) fetchChunk(req *Request, rep *Response) error {
	start := time.Now()
	err := c.client.Call("Server.ServeChunk", req, rep)
	dt := time.Now().Sub(start)
	c.store.AddTiming("FetchChunk", rep.Size, dt)
	return err
}

func (c *Client) fetch(want string, size int64) (bool, error) {
	chunkSize := defaultServeSize
	if int64(chunkSize) > size+1 {
		chunkSize = int(size + 1)
	}

	buf := make([]byte, chunkSize)

	var output *HashWriter
	written := 0

	var saved string
	for {
		req := &Request{
			Hash:  want,
			Start: written,
		}
		rep := &Response{Chunk: buf}
		err := c.fetchChunk(req, rep)
		if err != nil || !rep.Have {
			return false, err
		}

		// is this a bug in the rpc package?
		content := rep.Chunk[:rep.Size]

		if rep.Last && written == 0 {
			saved = c.store.Save(content)
			written = len(content)
			break
		} else if output == nil {
			output = c.store.NewHashWriter()
			defer output.Close()
		}

		n, err := output.Write(content)
		written += n
		if err != nil {
			return false, err
		}
		if rep.Last {
			break
		}
	}
	if output != nil {
		output.Close()
		saved = string(output.Sum())
	}
	c.store.addThroughput(int64(written), 0)
	if want != saved {
		log.Fatalf("file corruption: got %x want %x", saved, want)
	}
	return true, nil
}
