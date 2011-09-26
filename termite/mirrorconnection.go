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

	// For serving the Fileserver.
	reverseConnection net.Conn

	// Protected by mirrorConnections.Mutex.
	maxJobs       int
	availableJobs int

	// Any file updates that we should ship to the worker before
	// running any jobs.
	pendingChangesMutex sync.Mutex
	pendingChanges      []*FileAttr
}

func (me *mirrorConnection) queueFiles(files []*FileAttr) {
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

	keepAliveNs int64
	periodNs    int64

	wantedMaxJobs int

	stats *masterStats

	// Condition for mutex below.
	*sync.Cond

	// Protects all of the below.
	sync.Mutex
	workers      map[string]bool
	mirrors      map[string]*mirrorConnection
	lastActionNs int64
}

func (me *mirrorConnections) fetchWorkers() (newMap map[string]bool) {
	newMap = map[string]bool{}
	client, err := rpc.DialHTTP("tcp", me.coordinator)
	if err != nil {
		log.Println("dialing coordinator:", err)
		return newMap
	}
	defer client.Close()
	req := 0
	rep := Registered{}
	err = client.Call("Coordinator.List", &req, &rep)
	if err != nil {
		log.Println("coordinator rpc error:", err)
		return newMap
	}

	for _, v := range rep.Registrations {
		newMap[v.Address] = true
	}
	if len(newMap) == 0 {
		log.Println("coordinator has no workers for us.")
	}
	return newMap
}

func (me *mirrorConnections) refreshWorkers() {
	newWorkers := me.fetchWorkers()
	if len(newWorkers) > 0 {
		me.Mutex.Lock()
		defer me.Mutex.Unlock()
		me.workers = newWorkers
	}
}

func newMirrorConnections(m *Master, workers []string, coordinator string, maxJobs int) *mirrorConnections {
	me := &mirrorConnections{
		master:        m,
		wantedMaxJobs: maxJobs,
		workers:       make(map[string]bool),
		mirrors:       make(map[string]*mirrorConnection),
		coordinator:   coordinator,
		stats:         newMasterStats(),
	}
	me.setKeepAliveNs(60e9, 60e9)

	for _, w := range workers {
		me.workers[w] = true
	}
	if coordinator != "" {
		if workers != nil {
			log.Println("coordinator will overwrite workers.")
		}

		go me.periodicHouseholding()
	}
	me.Cond = sync.NewCond(&me.Mutex)
	return me
}

func (me *mirrorConnections) setKeepAliveNs(keepAliveNs float64, periodNs float64) {
	me.keepAliveNs = int64(keepAliveNs)
	me.periodNs = int64(periodNs)
}

func (me *mirrorConnections) periodicHouseholding() {
	me.refreshWorkers()
	for {
		c := time.After(me.periodNs)
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

	if me.lastActionNs+int64(me.keepAliveNs) > time.Nanoseconds() {
		return
	}

	log.Println("master inactive too long. Dropping connections.")
	me.dropConnections()
}

func (me *mirrorConnections) dropConnections() {
	for _, mc := range me.mirrors {
		mc.rpcClient.Close()
		mc.connection.Close()
		mc.reverseConnection.Close()
	}
	me.mirrors = make(map[string]*mirrorConnection)
	me.stats = newMasterStats()
}

func (me *mirrorConnections) queueFiles(origin *mirrorConnection, infos []*FileAttr) {
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

	for me.availableJobs() <= 0 {
		if len(me.workers) == 0 {
			me.workers = me.fetchWorkers()
		}
		me.tryConnect()
	
		if me.maxJobs() == 0 {
			// Didn't connect to anything.  Should
			// probably direct the wrapper to compile
			// locally.
			return nil, os.NewError("No workers found at all.")
		}
		
		if me.availableJobs() == 0 {
			me.Cond.Wait()
		}
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
	mc.reverseConnection.Close()
	me.mirrors[mc.workerAddr] = nil, false
	me.workers[mc.workerAddr] = false, false
	me.Cond.Broadcast()
}

func (me *mirrorConnections) jobDone(mc *mirrorConnection) {
	me.Mutex.Lock()
	defer me.Mutex.Unlock()

	me.lastActionNs = time.Nanoseconds()
	mc.availableJobs++
	me.Cond.Broadcast()
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
