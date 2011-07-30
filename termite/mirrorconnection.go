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

	// Any file updates that we should ship to the worker before
	// running any jobs.
	pendingChangesMutex sync.Mutex
	pendingChanges []AttrResponse
}

func (me *mirrorConnection) queueFiles(files []AttrResponse) {
	me.pendingChangesMutex.Lock()
	defer me.pendingChangesMutex.Unlock()
	for _, a := range files {
		me.pendingChanges = append(me.pendingChanges, a)
	}
}

func (me *mirrorConnection) sendFiles() os.Error {
	me.pendingChangesMutex.Lock()
	defer me.pendingChangesMutex.Unlock()

	req := UpdateRequest{
		Files: me.pendingChanges,
	}
	rep := UpdateResponse{}
	err := me.rpcClient.Call("Mirror.Update", &req, &rep)
	if err != nil {
		log.Println("Mirror.Update failure", err)
		return err
	}

	me.pendingChanges = me.pendingChanges[:0]
	return nil
}

// mirrorConnection manages connections from the master to the mirrors
// on the workers.
type mirrorConnections struct {
	master      *Master
	coordinator string

	keepAliveSeconds int64

	// Condition for mutex below.
	sync.Cond

	// Protects all of the below.
	sync.Mutex
	workers           map[string]bool
	mirrors           map[string]*mirrorConnection
	wantedMaxJobs     int
	lastActionSeconds int64
}

func (me *mirrorConnections) refreshWorkers() {
	client, err := rpc.DialHTTP("tcp", me.coordinator)
	if err != nil {
		log.Println("dialing coordinator:", err)
		return
	}
	defer client.Close()
	req := 0
	rep := Registered{}
	err = client.Call("Coordinator.List", &req, &rep)
	if err != nil {
		log.Println("coordinator rpc error:", err)
		return
	}

	newWorkers := map[string]bool{}
	for _, v := range rep.Registrations {
		newWorkers[v.Address] = true
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
		master:           m,
		wantedMaxJobs:    maxJobs,
		workers:          make(map[string]bool),
		mirrors:          make(map[string]*mirrorConnection),
		coordinator:      coordinator,
		keepAliveSeconds: 60,
	}
	for _, w := range workers {
		mc.workers[w] = true
	}
	if coordinator != "" {
		if workers != nil {
			log.Println("coordinator will overwrite workers.")
		}

		go mc.periodicHouseholding()
	}
	mc.Cond.L = &mc.Mutex
	return mc
}

func (me *mirrorConnections) periodicHouseholding() {
	me.refreshWorkers()
	for {
		c := time.After(me.keepAliveSeconds * 1e9)
		<-c
		me.refreshWorkers()
		me.maybeDropConnections()
	}
}

// Must be called with lock held.
func (me *mirrorConnections) availableJobs() int {
	a := 0
	for _, mc := range me.mirrors {
		a += mc.availableJobs
	}
	return a
}

// Must be called with lock held.
func (me *mirrorConnections) maxJobs() int {
	a := 0
	for _, mc := range me.mirrors {
		a += mc.maxJobs
	}
	return a
}

func (me *mirrorConnections) maybeDropConnections() {
	me.Mutex.Lock()
	defer me.Mutex.Unlock()

	// Already dropped everything.
	if len(me.mirrors) == 0 {
		return
	}

	// Something is running.
	if me.availableJobs() < me.maxJobs() {
		return
	}

	if me.lastActionSeconds+me.keepAliveSeconds > time.Seconds() {
		return
	}

	log.Println("master inactive too long. Dropping connections.")
	for _, mc := range me.mirrors {
		mc.connection.Close()
	}
	me.mirrors = make(map[string]*mirrorConnection)
}

func (me *mirrorConnections) queueFiles(origin *mirrorConnection, infos []AttrResponse) {
	for _, w := range me.mirrors {
		if origin != w {
			w.queueFiles(infos)
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

	log.Printf("Dropping mirror %s. Reason: %s", mc.workerAddr, err)
	mc.connection.Close()
	me.mirrors[mc.workerAddr] = nil, false
	me.workers[mc.workerAddr] = false, false
}

func (me *mirrorConnections) jobDone(mc *mirrorConnection) {
	me.Mutex.Lock()
	defer me.Mutex.Unlock()

	me.lastActionSeconds = time.Seconds()
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

	blacklist := []string{}
	for addr, _ := range me.workers {
		_, ok := me.mirrors[addr]
		if ok {
			continue
		}
		log.Printf("Creating mirror on %v, requesting %d jobs", addr, wanted)
		mc, err := me.master.createMirror(addr, wanted)
		if err != nil {
			log.Println("nonfatal error creating mirror:", err)
			blacklist = append(blacklist, addr)
			continue
		}
		mc.workerAddr = addr
		me.mirrors[addr] = mc
		break
	}

	for _, a := range blacklist {
		me.workers[a] = false, false
	}
}
