package termite

import (
	"log"
	"net"
	"os"
	"rpc"
	"sync"
	"time"
)

type mirrorConnection struct {
	workerAddr string // key in map.
	rpcClient  *rpc.Client
	connection net.Conn

	// Protected by mirrorConnections.Mutex.
	maxJobs       int
	availableJobs int
}

func (me *mirrorConnection) sendFiles(infos []AttrResponse) {
	req := UpdateRequest{
		Files: infos,
	}
	rep := UpdateResponse{}
	err := me.rpcClient.Call("Mirror.Update", &req, &rep)
	if err != nil {
		log.Println("Mirror.Update failure", err)
	}
}

type mirrorConnections struct {
	master      *Master
	coordinator string

	// Condition for mutex below.
	sync.Cond

	// Protects all of the below.
	sync.Mutex
	workers     []string
	mirrors       map[string]*mirrorConnection
	wantedMaxJobs int
}

func (me *mirrorConnections) refreshWorkers() {
	client, err := rpc.DialHTTP("tcp", me.coordinator)
	if err != nil {
		log.Println("dialing coordinator:", err)
		return
	}
	req := 0
	rep := Registered{}
	err = client.Call("Coordinator.List", &req, &rep)
	if err != nil {
		log.Println("coordinator rpc error:", err)
		return
	}

	newWorkers := []string{}
	for _, v := range rep.Registrations {
		newWorkers = append(newWorkers, v.Address)
	}
	if len(newWorkers) == 0 {
		log.Println("coordinator has no workers for us.")
		return
	}

	me.Mutex.Lock()
	defer me.Mutex.Unlock()
	me.workers = newWorkers
}


func newMirrorConnections(m *Master, workers []string, coordinator string, maxJobs int) *mirrorConnections {
	mc := &mirrorConnections{
		master:        m,
		wantedMaxJobs: maxJobs,
		workers:       workers,
		mirrors:       make(map[string]*mirrorConnection),
		coordinator:   coordinator,
	}
	if coordinator != "" {
		if workers != nil {
			log.Println("coordinator will overwrite workers.")
		}

		go mc.periodWorkersRefresh()
	}
	mc.Cond.L = &mc.Mutex
	return mc
}

const _WORKER_REFRESH_DELAY = 60

func (me *mirrorConnections) periodWorkersRefresh() {
	me.refreshWorkers()
	for {
		c := time.After(_WORKER_REFRESH_DELAY * 1e9)
		<-c
		me.refreshWorkers()
	}
}

func (me *mirrorConnections) availableJobs() int {
	a := 0
	for _, mc := range me.mirrors {
		a += mc.availableJobs
	}
	return a
}

func (me *mirrorConnections) maxJobs() int {
	a := 0
	for _, mc := range me.mirrors {
		a += mc.maxJobs
	}
	return a
}

func (me *mirrorConnections) broadcastFiles(origin *mirrorConnection, infos []AttrResponse) {
	for _, w := range me.mirrors {
		if origin != w {
			go w.sendFiles(infos)
		}
	}
}

// Gets a mirrorConnection to run on.  Will block if none available
func (me *mirrorConnections) pick() (*mirrorConnection, os.Error) {
	me.Mutex.Lock()
	defer me.Mutex.Unlock()

	if me.availableJobs() <= 0 {
		me.tryConnect()
	}

	if me.maxJobs() == 0 {
		// Didn't connect to anything.  Should
		// probably direct the wrapper to compile
		// locally.
		return nil, os.NewError("No workers found at all.")
	}

	for me.availableJobs() == 0 {
		me.Cond.Wait()
	}

	var found *mirrorConnection
	for _, v := range me.mirrors {
		if v.availableJobs > 0 {
			found = v
		}
	}
	found.availableJobs--
	return found, nil
}

func (me *mirrorConnections) drop(mc *mirrorConnection, err os.Error) {
	me.Mutex.Lock()
	defer me.Mutex.Unlock()

	// TODO - should blacklist the address.
	log.Printf("Dropping mirror %s. Reason: %s", mc.workerAddr, err)
	mc.connection.Close()
	me.mirrors[mc.workerAddr] = nil, false
}

func (me *mirrorConnections) jobDone(mc *mirrorConnection) {
	me.Mutex.Lock()
	defer me.Mutex.Unlock()

	mc.availableJobs++
	me.Cond.Signal()
}

// Tries to connect to one extra worker.  Must already hold mutex.
func (me *mirrorConnections) tryConnect() {
	// We want to max out capacity of each worker, as that helps
	// with cache hit rates on the worker.
	wanted := me.wantedMaxJobs - me.maxJobs()
	if wanted <= 0 {
		return
	}

	for _, addr := range me.workers {
		_, ok := me.mirrors[addr]
		if ok {
			continue
		}
		log.Println("Creating mirror on", addr)
		mc, err := me.master.createMirror(addr, wanted)
		if err != nil {
			log.Println("nonfatal error creating mirror:", err)
			continue
		}
		mc.workerAddr = addr
		me.mirrors[addr] = mc
	}
}
