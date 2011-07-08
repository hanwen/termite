package main

import (
	"os"
	"http"
	"net"
	"time"
	"log"
	"rpc"
	"sync"
	"github.com/hanwen/go-fuse/termite"
)

type Worker struct {
	termite.Registration
	LastReported int64
}

type Coordinator struct {
	sync.Mutex
	workers map[string]*Worker
}

func NewCoordinator() *Coordinator {
	return &Coordinator{
		workers: make(map[string]*Worker),
	}
}

func Reachable(addr string) bool {
	conn, err := net.Dial("tcp", addr)
	conn.Close()
	return err == nil
}

func (me *Coordinator) Register(req *termite.Registration, rep *int) os.Error {
	if !Reachable(req.Address) {
		return os.NewError("error contacting address")
	}

	me.Mutex.Lock()
	defer me.Mutex.Unlock()

	w := &Worker{*req, 0}
	w.LastReported = time.Seconds()
	me.workers[w.Address] = w
	return nil
}

func (me *Coordinator) CheckReachable() {
	now := time.Seconds()
	
	me.Mutex.Lock()
	var addrs []string
	for k, _ := range me.workers {
		addrs = append(addrs, k)
	}
	me.Mutex.Unlock()
	
	var toDelete []string
	for _, a := range addrs {
		if !Reachable(a) {
			toDelete = append(toDelete, a)
		}
	}
	
	me.Mutex.Lock()
	for _, a := range toDelete {
		if me.workers[a].LastReported < now {
			me.workers[a] = nil, false
		}
	}
	me.Mutex.Unlock()
}

const _POLL = 5 * 60 *1e9

func (me *Coordinator) PeriodicCheck() {
	me.CheckReachable()
	time.AfterFunc(_POLL, func() { me.PeriodicCheck() })
}

func handler(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "text/plain")
	w.Write([]byte("This is an example server.\n"))
}

func main() {
	c := &Coordinator{}
	http.HandleFunc("/", handler)
	rpc.Register(c)
	rpc.HandleHTTP()

	addr :=":12345"
	log.Println("Listening on", addr)
	err := http.ListenAndServe(addr, nil)
	if err != nil {
		log.Fatal("ListenAndServe: ", err.String())
	}
}
