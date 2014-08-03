package termite

import (
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/rpc"
	"strings"
	"sync"
	"time"

	"github.com/hanwen/termite/attr"
	"github.com/hanwen/termite/cba"
	"github.com/hanwen/termite/stats"
)

type mirrorConnection struct {
	workerAddr    string // key in map.
	rpcClient     *rpc.Client
	contentClient *cba.Client

	// For serving the Fileserver.
	reverseConnection  io.ReadWriteCloser
	reverseContentConn io.ReadWriteCloser

	// Protected by mirrorConnections.Mutex.
	maxJobs       int
	availableJobs int

	master        *Master
	fileSetWaiter *attr.FileSetWaiter
}

func (c *mirrorConnection) Id() string {
	return c.workerAddr
}

func (c *mirrorConnection) replay(fset attr.FileSet) error {
	// Must get data before we modify the file-system, so we don't
	// leave the FS in a half-finished state.
	for _, info := range fset.Files {
		if info.Hash != "" && !c.master.contentStore.Has(info.Hash) {
			got, err := c.contentClient.Fetch(info.Hash, int64(info.Size))
			if !got && err == nil {
				log.Fatalf("mirrorConnection.replay: fetch corruption remote does not have file %x", info.Hash)
			}
			if err != nil {
				return err
			}
		}
	}
	c.master.replay(fset)
	return nil
}

func (c *mirrorConnection) Send(files []*attr.FileAttr) error {
	req := UpdateRequest{
		Files: files,
	}
	rep := UpdateResponse{}
	err := c.rpcClient.Call("Mirror.Update", &req, &rep)
	if err != nil {
		log.Println("Mirror.Update failure", err)
		return err
	}
	log.Printf("Sent pending changes to %s", c.workerAddr)
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

func (c *mirrorConnections) fetchWorkers(last *time.Time) (newMap map[string]bool, err error) {
	newMap = map[string]bool{}
	client, err := rpc.DialHTTP("tcp", c.coordinator)
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

func (c *mirrorConnections) refreshWorkers() {
	last := time.Unix(0, 0)
	for {
		newWorkers, err := c.fetchWorkers(&last)
		if err != nil {
			time.Sleep(10 * time.Second)
			continue
		}
		//log.Printf("Got %d workers %v", len(newWorkers), last)
		c.Mutex.Lock()
		c.workers = newWorkers
		c.Mutex.Unlock()
	}
}

func newMirrorConnections(m *Master, coordinator string, maxJobs int) *mirrorConnections {
	c := &mirrorConnections{
		master:        m,
		wantedMaxJobs: maxJobs,
		workers:       make(map[string]bool),
		mirrors:       make(map[string]*mirrorConnection),
		coordinator:   coordinator,
		keepAlive:     time.Minute,
	}
	c.refreshStats()
	return c
}

func (c *mirrorConnections) refreshStats() {
	c.stats = stats.NewServerStats()
	c.stats.PhaseOrder = []string{"run", "send", "remote", "filewait"}
}

func (c *mirrorConnections) periodicHouseholding() {
	c.maybeDropConnections()
}

// Must be called with lock held.
func (c *mirrorConnections) availableJobs() int {
	a := 0
	for _, mc := range c.mirrors {
		if mc.availableJobs > 0 {
			a += mc.availableJobs
		}
	}
	return a
}

// Must be called with lock held.
func (c *mirrorConnections) maxJobs() int {
	a := 0
	for _, mc := range c.mirrors {
		a += mc.maxJobs
	}
	return a
}

func (c *mirrorConnections) maybeDropConnections() {
	c.Mutex.Lock()
	defer c.Mutex.Unlock()

	// Already dropped everything.
	if len(c.mirrors) == 0 {
		return
	}

	// Something is running.
	if c.availableJobs() < c.maxJobs() {
		return
	}

	if c.lastActionTime.Add(c.keepAlive).After(time.Now()) {
		return
	}

	log.Println("master inactive too long. Dropping connections.")
	c.dropConnections()
}

func (c *mirrorConnections) dropConnections() {
	for _, mc := range c.mirrors {
		mc.rpcClient.Close()
		mc.contentClient.Close()
		mc.reverseConnection.Close()
		mc.reverseContentConn.Close()
		c.master.attributes.RmClient(mc)
	}
	c.mirrors = make(map[string]*mirrorConnection)
	c.refreshStats()
}

// Gets a mirrorConnection to run on.  Will block if none available
func (c *mirrorConnections) find(name string) (*mirrorConnection, error) {
	c.Mutex.Lock()
	defer c.Mutex.Unlock()

	var found *mirrorConnection
	for nm, v := range c.mirrors {
		if strings.Contains(nm, name) {
			found = v
			break
		}
	}
	if found == nil {
		return nil, fmt.Errorf("No worker with name: %q. Have %v", name, c.mirrors)
	}
	found.availableJobs--
	return found, nil
}

func (c *mirrorConnections) pick() (*mirrorConnection, error) {
	c.Mutex.Lock()
	defer c.Mutex.Unlock()

	if c.availableJobs() <= 0 {
		c.tryConnect()

		if c.maxJobs() == 0 {
			// Didn't connect to anything.  Should
			// probably direct the wrapper to compile
			// locally.
			return nil, errors.New("No workers found at all.")
		}
	}

	maxAvail := -1e9
	var maxAvailMirror *mirrorConnection
	for _, v := range c.mirrors {
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

func (c *mirrorConnections) drop(mc *mirrorConnection, err error) {
	c.master.attributes.RmClient(mc)

	c.Mutex.Lock()
	defer c.Mutex.Unlock()
	log.Printf("Dropping mirror %s. Reason: %s", mc.workerAddr, err)
	mc.rpcClient.Close()
	mc.contentClient.Close()
	mc.reverseConnection.Close()
	mc.reverseContentConn.Close()
	delete(c.mirrors, mc.workerAddr)
	delete(c.workers, mc.workerAddr)
}

func (c *mirrorConnections) jobDone(mc *mirrorConnection) {
	c.Mutex.Lock()
	defer c.Mutex.Unlock()

	c.lastActionTime = time.Now()
	mc.availableJobs++
}

func (c *mirrorConnections) idleWorkerAddress() string {
	cands := []string{}
	for addr := range c.workers {
		_, ok := c.mirrors[addr]
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
func (c *mirrorConnections) tryConnect() {
	// We want to max out capacity of each worker, as that helps
	// with cache hit rates on the worker.
	wanted := c.wantedMaxJobs - c.maxJobs()
	if wanted <= 0 {
		return
	}

	for {
		addr := c.idleWorkerAddress()
		if addr == "" {
			break
		}
		c.Mutex.Unlock()
		log.Printf("Creating mirror on %v, requesting %d jobs", addr, wanted)
		mc, err := c.master.createMirror(addr, wanted)
		c.Mutex.Lock()
		if err != nil {
			delete(c.workers, addr)
			log.Println("nonfatal error creating mirror:", err)
		} else {
			// This could happen in the unlikely event of
			// the workers having more capacity than our
			// parallelism.
			if _, ok := c.mirrors[addr]; ok {
				log.Panicf("already have this mirror: %v", addr)
			}
			mc.workerAddr = addr
			c.mirrors[addr] = mc
			c.master.attributes.AddClient(mc)
		}
	}
}
