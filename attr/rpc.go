package attr

import (
	"fmt"
	"io"
	"log"
	"net/rpc"
	"time"

	"github.com/hanwen/termite/stats"
)

type AttrRequest struct {
	Name string

	// Worker asking for the request. Useful for debugging.
	Origin string
}

type AttrResponse struct {
	Attrs []*FileAttr
}

// Client is an RPC client for a remote AttrCache.
type Client struct {
	client  *rpc.Client
	id      string
	timings *stats.TimerStats
}

// NewClient instantiates a Client. The ID string is used for
// identifying the client in remote logs.
func NewClient(c io.ReadWriteCloser, id string) *Client {
	return &Client{
		client:  rpc.NewClient(c),
		id:      id,
		timings: stats.NewTimerStats(),
	}
}

func (c *Client) Close() {
	c.client.Close()
}

// GetAttr returns the attributes for a path.
func (c *Client) GetAttr(n string, wanted *FileAttr) error {
	req := &AttrRequest{
		Name:   n,
		Origin: c.id,
	}
	start := time.Now()
	rep := &AttrResponse{}
	err := c.client.Call("Server.GetAttr", req, rep)
	dt := time.Now().Sub(start)
	c.timings.Log("Client.GetAttr", dt)

	for _, attr := range rep.Attrs {
		if attr.Path == n {
			*wanted = *attr
			break
		}
	}

	return err
}

type Server struct {
	attributes *AttributeCache
	stats      *stats.TimerStats
}

// NewServer instantiates an RPC server for the given
// AttributeCache. If timings is optional and will be used for
// recording timing data.
func NewServer(a *AttributeCache, timings *stats.TimerStats) *Server {
	if timings == nil {
		timings = stats.NewTimerStats()
	}
	me := &Server{
		attributes: a,
		stats:      timings,
	}

	return me
}

// ServeRPC starts an RPC server on rwc. It should typically be used
// in a goroutine.
func ServeRPC(s *Server, rwc io.ReadWriteCloser) {
	rs := rpc.NewServer()
	rs.Register(s)
	rs.ServeConn(rwc)
}

// GetAttr is an RPC entry point. The name in AttrRequest should be absolute
func (s *Server) GetAttr(req *AttrRequest, rep *AttrResponse) error {
	start := time.Now()
	log.Printf("GetAttr %s req %q", req.Origin, req.Name)
	if req.Name != "" && req.Name[0] == '/' {
		return fmt.Errorf("name %q starts with /", req.Name)
	}

	a := s.attributes.GetDir(req.Name)
	if a.Hash != "" {
		log.Printf("GetAttr %v", a)
	}
	rep.Attrs = append(rep.Attrs, a)
	dt := time.Now().Sub(start)
	s.stats.Log("Server.GetAttr", dt)
	return nil
}
