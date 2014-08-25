package termite

import (
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"sort"
	"sync"
	"time"
)

var _ = log.Println

type Registration struct {
	Address        string
	Name           string
	Version        string
	HttpStatusPort int
}

type RegistrationRequest Registration

type ListRequest struct {
	// Return changes after this time stamp.  Will halt if no
	// changes to report.
	Latest time.Time
}

type ListResponse struct {
	Registrations []Registration
	LastChange    time.Time
}

type WorkerRegistration struct {
	Registration
	LastReported time.Time
}

// Coordinator implements CoordinatorService RPC interface for termite.  Workers
// register here.  A master looking for workers contacts the
// Coordinator to fetch a list of available workers.  In addition, it
// has a HTTP interface to inspect each worker.
type Coordinator struct {
	Mux *http.ServeMux

	options *CoordinatorOptions

	listener net.Listener

	dialer     connDialer
	mutex      sync.Mutex
	cond       *sync.Cond
	workers    map[string]*WorkerRegistration
	lastChange time.Time
}

// RPC interface for Coordinator
type CoordinatorService Coordinator

func (cs *CoordinatorService) Register(req *RegistrationRequest, rep *Empty) error {
	return ((*Coordinator)(cs)).Register(req, rep)
}

func (cs *CoordinatorService) List(req *ListRequest, rep *ListResponse) error {
	return ((*Coordinator)(cs)).List(req, rep)
}

type CoordinatorOptions struct {
	// Secret is the password for coordinator, workers and master
	// to authenticate.
	Secret []byte

	// Password should be passed in the kill/restart URLs to make
	// sure web scrapers don't randomly shutdown workers.
	WebPassword string
}

func NewCoordinator(opts *CoordinatorOptions) *Coordinator {
	o := *opts
	c := &Coordinator{
		options: &o,
		workers: make(map[string]*WorkerRegistration),
		Mux:     http.NewServeMux(),
		dialer:  newTCPDialer(o.Secret),
	}
	c.cond = sync.NewCond(&c.mutex)
	return c
}

func (c *Coordinator) Register(req *RegistrationRequest, rep *Empty) error {
	rwc, err := c.dialWorker(req.Address)
	if err != nil {
		return fmt.Errorf(
			"Dial(%v).Open(%q): %v", req.Address, RPC_CHANNEL, err)
	}

	rwc.Close()

	c.mutex.Lock()
	defer c.mutex.Unlock()

	w := &WorkerRegistration{Registration: Registration(*req)}
	w.LastReported = time.Now()
	c.lastChange = w.LastReported
	c.workers[w.Address] = w
	c.cond.Broadcast()
	return nil
}

func (c *Coordinator) WorkerCount() int {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	return len(c.workers)
}

func (c *Coordinator) List(req *ListRequest, rep *ListResponse) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	for !c.lastChange.After(req.Latest) {
		c.cond.Wait()
	}
	keys := []string{}
	for k := range c.workers {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		w := c.workers[k]
		rep.Registrations = append(rep.Registrations, w.Registration)
	}
	rep.LastChange = c.lastChange
	return nil
}

func (c *Coordinator) dialWorker(address string) (io.ReadWriteCloser, error) {
	mux, err := c.dialer.Dial(address)
	if err != nil {
		return nil, err
	}

	return mux.Open(RPC_CHANNEL)
}

func (c *Coordinator) checkReachable() {
	now := time.Now()

	addrs := c.workerAddresses()

	var toDelete []string
	for _, a := range addrs {
		conn, err := c.dialWorker(a)
		if err != nil {
			toDelete = append(toDelete, a)
		} else {
			conn.Close()
		}
	}

	if len(toDelete) == 0 {
		return
	}

	c.mutex.Lock()
	for _, a := range toDelete {
		w := c.workers[a]
		if w != nil && now.After(w.LastReported) {
			log.Println("dropping worker", a)
			delete(c.workers, a)
		}
	}
	c.lastChange = time.Now()
	c.mutex.Unlock()
}

const _POLL = 60

func (c *Coordinator) PeriodicCheck() {
	for {
		time.Sleep(_POLL * time.Second)
		c.checkReachable()
	}
}

func (c *Coordinator) killWorker(addr string, restart bool) error {
	conn, err := c.dialWorker(addr)
	if err == nil {
		killReq := ShutdownRequest{Restart: restart}
		rep := ShutdownResponse{}
		cl := rpc.NewClient(conn)
		defer cl.Close()
		err = cl.Call("Worker.Shutdown", &killReq, &rep)
	}
	return err
}

func (c *Coordinator) shutdownWorker(addr string, restart bool) error {
	conn, err := c.dialWorker(addr)
	if err != nil {
		return err
	}

	killReq := ShutdownRequest{Restart: restart}
	rep := ShutdownResponse{}
	cl := rpc.NewClient(conn)
	err = cl.Call("Worker.Shutdown", &killReq, &rep)
	cl.Close()
	conn.Close()
	return err
}

func (c *Coordinator) workerAddresses() (out []string) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	for k := range c.workers {
		out = append(out, k)
	}
	return out
}

func (c *Coordinator) getWorker(addr string) *WorkerRegistration {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	return c.workers[addr]
}

func (c *Coordinator) Shutdown() {
	log.Println("Coordinator shutdown.")
	c.listener.Close()
}

func (c *Coordinator) log(req *http.Request) {
	log.Printf("from %v: %v", req.RemoteAddr, req.URL)
}
