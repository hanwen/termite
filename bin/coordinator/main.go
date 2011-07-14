package main

import (
	"flag"
	"fmt"
	"os"
	"http"
	"net"
	"time"
	"log"
	"rpc"
	"sync"
	"github.com/hanwen/termite/termite"
)

type Worker struct {
	termite.Registration
	LastReported int64
}

type Coordinator struct {
	mutex   sync.Mutex
	workers map[string]*Worker
}

func NewCoordinator() *Coordinator {
	return &Coordinator{
		workers: make(map[string]*Worker),
	}
}

func (me *Coordinator) Register(req *termite.Registration, rep *int) os.Error {
	if !reachable(req.Address) {
		return os.NewError("error contacting address")
	}

	me.mutex.Lock()
	defer me.mutex.Unlock()

	w := &Worker{*req, 0}
	w.LastReported = time.Seconds()
	me.workers[w.Address] = w
	return nil
}

func (me *Coordinator) List(req *int, rep *termite.Registered) os.Error {
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

func (me *Coordinator) periodicCheck() {
	for {
		c := time.After(_POLL * 1e9)
		<-c
		me.checkReachable()
	}
}

func (me *Coordinator) htmlHandler(w http.ResponseWriter, req *http.Request) {
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

func main() {
	port := flag.Int("port", 1233, "Where to listen for work requests.")
	flag.Parse()

	c := NewCoordinator()
	http.HandleFunc("/",
		func(w http.ResponseWriter, req *http.Request) {
			c.htmlHandler(w, req)
		})

	rpc.Register(c)
	rpc.HandleHTTP()

	go c.periodicCheck()
	addr := fmt.Sprintf(":%d", *port)
	log.Println("Listening on", addr)
	err := http.ListenAndServe(addr, nil)
	if err != nil {
		log.Fatal("ListenAndServe: ", err.String())
	}
}
