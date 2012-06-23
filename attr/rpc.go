package attr

import (
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

type Client struct {
	client  *rpc.Client
	id      string
	timings *stats.TimerStats
}

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

func NewServer(a *AttributeCache) *Server {
	me := &Server{
		attributes: a,
		stats:      stats.NewTimerStats(),
	}

	return me
}

func (s *Server) TimingMessages() []string {
	return s.stats.TimingMessages()
}

func (s *Server) GetAttr(req *AttrRequest, rep *AttrResponse) error {
	start := time.Now()
	log.Printf("GetAttr %s req %q", req.Origin, req.Name)
	if req.Name != "" && req.Name[0] == '/' {
		panic("leading /")
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
