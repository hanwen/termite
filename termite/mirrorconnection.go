package termite

import (
	"errors"
	"fmt"
	"github.com/hanwen/termite/attr"
	"github.com/hanwen/termite/cba"
	"github.com/hanwen/termite/stats"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"strings"
	"sync"
	"time"
)

type mirrorConnection struct {
	workerAddr    string // key in map.
	rpcClient     *rpc.Client
	contentClient *cba.Client

	// For serving the Fileserver.
	reverseConnection  net.Conn
	reverseContentConn net.Conn

	// Protected by mirrorConnections.Mutex.
	maxJobs       int
	availableJobs int

	master        *Master
	fileSetWaiter *attr.FileSetWaiter
}

func (me *mirrorConnection) Id() string {
	return me.workerAddr
}

func (me *mirrorConnection) replay(fset attr.FileSet) error {
	// Must get data before we modify the file-system, so we don't
	// leave the FS in a half-finished state.
	for _, info := range fset.Files {
		if info.Hash != "" && !me.master.contentStore.HasHash(info.Hash) {
			got, err := me.contentClient.Fetch(info.Hash, int64(info.Size))
			if !got && err == nil {
				log.Fatalf("mirrorConnection.replay: fetch corruption remote does not have file %x", info.Hash)
			}
			if err != nil {
				return err
			}
		}
	}
	me.master.replay(fset)
	return nil
}

func (me *mirrorConnection) Send(files []*attr.FileAttr) error {
	req := UpdateRequest{
		Files: files,
	}
	rep := UpdateResponse{}
	err := me.rpcClient.Call("Mirror.Update", &req, &rep)
	if err != nil {
		log.Println("Mirror.Update failure", err)
		return err
	}
	log.Printf("Sent pending changes to %s", me.workerAddr)
	return nil
}

// mirrorConnection manages connections from the master to the mirrors
// on the workers.
type mirrorConnections struct {
	master      *Master
	coordinator string

	keepAlive time.Duration

	wantedMaxJobs int

	stats *stats.ServerStats

	// Protects all of the below.
	sync.Mutex
	workers        map[string]bool
	mirrors        map[string]*mirrorConnection
	lastActionTime time.Time
}

func (me *mirrorConnections) fetchWorkers(last *time.Time) (newMap map[string]bool, err error) {
	newMap = map[string]bool{}
	client, err := rpc.DialHTTP("tcp", me.coordinator)
	if err != nil {
		log.Println("fetchWorkers: dialing coordinator:", err)
		return nil, err
	}
	defer client.Close()
	req := ListRequest{Latest: *last}
	rep := ListResponse{}
	err = client.Call("Coordinator.List", &req, &rep)
	if err != nil {
		log.Println("coordinator rpc error:", err)
		return nil, err
	}
	
	for _, v := range rep.Registrations {
		newMap[v.Address] = true
	}
	if len(newMap) == 0 {
		log.Println("coordinator has no workers for us.")
	}
	*last = rep.LastChange
	return newMap, nil
}

func (me *mirrorConnections) refreshWorkers() {
	last := time.Unix(0, 0)
	for {
		newWorkers, err := me.fetchWorkers(&last)
		if err != nil {
			time.Sleep(10*time.Second)
			continue
		}
		log.Printf("Got %d workers %v", len(newWorkers), last)
		me.Mutex.Lock()
		me.workers = newWorkers
		me.Mutex.Unlock()
	}
}

func newMirrorConnections(m *Master, coordinator string, maxJobs int) *mirrorConnections {
	me := &mirrorConnections{
		master:        m,
		wantedMaxJobs: maxJobs,
		workers:       make(map[string]bool),
		mirrors:       make(map[string]*mirrorConnection),
		coordinator:   coordinator,
		keepAlive:     time.Minute,
	}
	me.refreshStats()
	return me
}

func (me *mirrorConnections) refreshStats() {
	me.stats = stats.NewServerStats()
	me.stats.PhaseOrder = []string{"run", "send", "remote", "filewait"}
}

func (me *mirrorConnections) periodicHouseholding() {
	me.maybeDropConnections()
}

// Must be called with lock held.
func (me *mirrorConnections) availableJobs() int {
	a := 0
	for _, mc := range me.mirrors {
		if mc.availableJobs > 0 {
			a += mc.availableJobs
		}
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

	if me.lastActionTime.Add(me.keepAlive).After(time.Now()) {
		return
	}

	log.Println("master inactive too long. Dropping connections.")
	me.dropConnections()
}

func (me *mirrorConnections) dropConnections() {
	for _, mc := range me.mirrors {
		mc.rpcClient.Close()
		mc.contentClient.Close()
		mc.reverseConnection.Close()
		mc.reverseContentConn.Close()
		me.master.attributes.RmClient(mc)
	}
	me.mirrors = make(map[string]*mirrorConnection)
	me.refreshStats()
}

// Gets a mirrorConnection to run on.  Will block if none available
func (me *mirrorConnections) find(name string) (*mirrorConnection, error) {
	me.Mutex.Lock()
	defer me.Mutex.Unlock()

	var found *mirrorConnection
	for nm, v := range me.mirrors {
		if strings.Contains(nm, name) {
			found = v
			break
		}
	}
	if found == nil {
		return nil, fmt.Errorf("No worker with name: %q. Have %v", name, me.mirrors)
	}
	found.availableJobs--
	return found, nil
}

func (me *mirrorConnections) pick() (*mirrorConnection, error) {
	me.Mutex.Lock()
	defer me.Mutex.Unlock()

	if me.availableJobs() <= 0 {
		me.tryConnect()

		if me.maxJobs() == 0 {
			// Didn't connect to anything.  Should
			// probably direct the wrapper to compile
			// locally.
			return nil, errors.New("No workers found at all.")
		}
	}

	maxAvail := -1e9
	var maxAvailMirror *mirrorConnection
	for _, v := range me.mirrors {
		if v.availableJobs > 0 {
			v.availableJobs--
			return v, nil
		}
		l := float64(v.availableJobs) / float64(v.maxJobs)
		if l > maxAvail {
			maxAvailMirror = v
			maxAvail = l
		}
	}

	maxAvailMirror.availableJobs--
	return maxAvailMirror, nil
}

func (me *mirrorConnections) drop(mc *mirrorConnection, err error) {
	me.master.attributes.RmClient(mc)

	me.Mutex.Lock()
	defer me.Mutex.Unlock()
	log.Printf("Dropping mirror %s. Reason: %s", mc.workerAddr, err)
	mc.rpcClient.Close()
	mc.contentClient.Close()
	mc.reverseConnection.Close()
	mc.reverseContentConn.Close()
	delete(me.mirrors, mc.workerAddr)
	delete(me.workers, mc.workerAddr)
}

func (me *mirrorConnections) jobDone(mc *mirrorConnection) {
	me.Mutex.Lock()
	defer me.Mutex.Unlock()

	me.lastActionTime = time.Now()
	mc.availableJobs++
}

func (me *mirrorConnections) idleWorkerAddress() string {
	cands := []string{}
	for addr := range me.workers {
		_, ok := me.mirrors[addr]
		if ok {
			continue
		}
		cands = append(cands, addr)
	}

	if len(cands) == 0 {
		return ""
	}
	return cands[rand.Intn(len(cands))]
}

// Tries to connect to one extra worker.  Must already hold mutex.
func (me *mirrorConnections) tryConnect() {
	// We want to max out capacity of each worker, as that helps
	// with cache hit rates on the worker.
	wanted := me.wantedMaxJobs - me.maxJobs()
	if wanted <= 0 {
		return
	}

	for {
		addr := me.idleWorkerAddress()
		if addr == "" {
			break
		}
		me.Mutex.Unlock()
		log.Printf("Creating mirror on %v, requesting %d jobs", addr, wanted)
		mc, err := me.master.createMirror(addr, wanted)
		me.Mutex.Lock()
		if err != nil {
			delete(me.workers, addr)
			log.Println("nonfatal error creating mirror:", err)
		} else {
			// This could happen in the unlikely event of
			// the workers having more capacity than our
			// parallelism.
			if _, ok := me.mirrors[addr]; ok {
				log.Panicf("already have this mirror: %v", addr)
			}
			mc.workerAddr = addr
			me.mirrors[addr] = mc
			me.master.attributes.AddClient(mc)
		}
	}
}
