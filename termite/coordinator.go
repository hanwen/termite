package termite

import (
	"fmt"
	"os"
	"http"
	"net"
	"time"
	"sync"
)

type WorkerRegistration struct {
	Registration
	LastReported int64
}

type Coordinator struct {
	mutex   sync.Mutex
	workers map[string]*WorkerRegistration
}

func NewCoordinator() *Coordinator {
	return &Coordinator{
		workers: make(map[string]*WorkerRegistration),
	}
}

func (me *Coordinator) Register(req *Registration, rep *int) os.Error {
	if !reachable(req.Address) {
		return os.NewError("error contacting address")
	}

	me.mutex.Lock()
	defer me.mutex.Unlock()

	w := &WorkerRegistration{*req, 0}
	w.LastReported = time.Seconds()
	me.workers[w.Address] = w
	return nil
}

func (me *Coordinator) List(req *int, rep *Registered) os.Error {
	me.mutex.Lock()
	defer me.mutex.Unlock()

	for _, w := range me.workers {
		rep.Registrations = append(rep.Registrations, w.Registration)
	}
	return nil
}

func reachable(addr string) bool {
	conn, _ := net.Dial("tcp", addr)
	ok := conn != nil
	if ok {
		conn.Close()
	}
	return ok
}


func (me *Coordinator) checkReachable() {
	now := time.Seconds()

	me.mutex.Lock()
	var addrs []string
	for k, _ := range me.workers {
		addrs = append(addrs, k)
	}
	me.mutex.Unlock()

	var toDelete []string
	for _, a := range addrs {
		if !reachable(a) {
			toDelete = append(toDelete, a)
		}
	}

	me.mutex.Lock()
	for _, a := range toDelete {
		if me.workers[a].LastReported < now {
			me.workers[a] = nil, false
		}
	}
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

func (me *Coordinator) HtmlHandler(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "text/html")
	me.mutex.Lock()
	defer me.mutex.Unlock()

	fmt.Fprintf(w, "<html><head><title>Termite coordinator</title></head>")
	fmt.Fprintf(w, "<body><h1>Termite coordinator</h1>")

	for _, worker := range me.workers {
		fmt.Fprintf(w, "<p>address <tt>%s</tt>, host <tt>%s</tt>", worker.Address, worker.Name)
	}
	fmt.Fprintf(w, "</body></html>")
}

