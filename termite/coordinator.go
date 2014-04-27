package termite

import (
	"errors"
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

// Coordinator is the registration service for termite.  Workers
// register here.  A master looking for workers contacts the
// Coordinator to fetch a list of available workers.  In addition, it
// has a HTTP interface to inspect each worker.
type Coordinator struct {
	Mux *http.ServeMux

	options *CoordinatorOptions

	listener net.Listener

	mutex      sync.Mutex
	cond       *sync.Cond
	workers    map[string]*WorkerRegistration
	lastChange time.Time
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
	}
	c.cond = sync.NewCond(&c.mutex)
	return c
}

func (me *Coordinator) Register(req *RegistrationRequest, rep *Empty) error {
	conn, err := me.dial(req.Address)
	if conn != nil {
		conn.Close()
	}
	if err != nil {
		return errors.New(fmt.Sprintf(
			"error contacting address: %v", err))
	}

	me.mutex.Lock()
	defer me.mutex.Unlock()

	w := &WorkerRegistration{Registration: Registration(*req)}
	w.LastReported = time.Now()
	me.lastChange = w.LastReported
	me.workers[w.Address] = w
	me.cond.Broadcast()
	return nil
}

func (me *Coordinator) WorkerCount() int {
	me.mutex.Lock()
	defer me.mutex.Unlock()
	return len(me.workers)
}

func (me *Coordinator) List(req *ListRequest, rep *ListResponse) error {
	me.mutex.Lock()
	defer me.mutex.Unlock()

	for !me.lastChange.After(req.Latest) {
		me.cond.Wait()
	}

	keys := []string{}
	for k := range me.workers {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		w := me.workers[k]
		rep.Registrations = append(rep.Registrations, w.Registration)
	}
	rep.LastChange = me.lastChange
	return nil
}

func (me *Coordinator) checkReachable() {
	now := time.Now()

	addrs := me.workerAddresses()

	var toDelete []string
	for _, a := range addrs {
		conn, err := net.Dial("tcp", a)
		if err != nil {
			toDelete = append(toDelete, a)
		} else {
			conn.Close()
		}
	}

	if len(toDelete) == 0 {
		return
	}

	me.mutex.Lock()
	for _, a := range toDelete {
		w := me.workers[a]
		if w != nil && now.After(w.LastReported) {
			delete(me.workers, a)
		}
	}
	me.lastChange = time.Now()
	me.mutex.Unlock()
}

const _POLL = 60

func (me *Coordinator) PeriodicCheck() {
	for {
		c := time.After(_POLL * 1e9)
		<-c
		me.checkReachable()
	}
}

func (c *Coordinator) dial(addr string) (io.ReadWriteCloser, error) {
	return DialTypedConnection(addr, RPC_CHANNEL, c.options.Secret)
}

func (c *Coordinator) killWorker(addr string, restart bool) error {
	conn, err := c.dial(addr)
	if err == nil {
		log.Println("calling kill")
		killReq := ShutdownRequest{Restart: restart}
		rep := ShutdownResponse{}
		cl := rpc.NewClient(conn)
		defer cl.Close()
		err = cl.Call("Worker.Shutdown", &killReq, &rep)
	}
	return err
}

func (c *Coordinator) shutdownWorker(addr string, restart bool) error {
	conn, err := c.dial(addr)
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

func (me *Coordinator) workerAddresses() (out []string) {
	me.mutex.Lock()
	defer me.mutex.Unlock()
	for k := range me.workers {
		out = append(out, k)
	}
	return out
}

func (me *Coordinator) getWorker(addr string) *WorkerRegistration {
	me.mutex.Lock()
	defer me.mutex.Unlock()
	return me.workers[addr]
}

func (me *Coordinator) Shutdown() {
	log.Println("Coordinator shutdown.")
	me.listener.Close()
}

func (me *Coordinator) log(req *http.Request) {
	log.Printf("from %v: %v", req.RemoteAddr, req.URL)
}
