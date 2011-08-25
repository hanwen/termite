package termite

import (
	"fmt"
	"log"
	"net"
	"os"
	"rpc"
	"sync"
	"strings"
	"runtime"
	"time"
)

var _ = log.Println

type WorkReply struct {
	Exit   *os.Waitmsg
	Files  []FileAttr
	Stderr string
	Stdout string
}

type WorkRequest struct {
	Prefetch []FileAttr

	// Id of connection streaming stdin.
	StdinId      string
	Debug        bool
	WritableRoot string
	Binary       string
	Argv         []string
	Env          []string
	Dir          string
	RanLocally   bool
}

func (me *WorkRequest) Summary() string {
	return fmt.Sprintf("stdin %s cmd %s", me.StdinId, me.Argv)
}

type WorkerDaemon struct {
	secret         []byte

	rpcServer      *rpc.Server
	contentCache   *ContentCache
	contentServer  *ContentServer
	maxJobCount    int
	pending        *PendingConnections
	cacheDir       string
	tmpDir         string

	stopListener   chan int

	mirrorMapMutex sync.Mutex
	cond           *sync.Cond
	mirrorMap      map[string]*Mirror
	shuttingDown   bool
}

func (me *WorkerDaemon) getMirror(rpcConn, revConn net.Conn, reserveCount int) (*Mirror, os.Error) {
	if reserveCount <= 0 {
		return nil, os.NewError("must ask positive jobcount")
	}
	me.mirrorMapMutex.Lock()
	defer me.mirrorMapMutex.Unlock()
	used := 0
	for _, v := range me.mirrorMap {
		used += v.maxJobCount
	}

	remaining := me.maxJobCount - used
	if remaining <= 0 {
		return nil, os.NewError("no processes available")
	}
	if remaining < reserveCount {
		reserveCount = remaining
	}

	mirror := NewMirror(me, rpcConn, revConn)
	mirror.maxJobCount = reserveCount
	key := fmt.Sprintf("%v", rpcConn.RemoteAddr())
	me.mirrorMap[key] = mirror
	mirror.key = key
	return mirror, nil
}

func NewWorkerDaemon(secret []byte, tmpDir string, cacheDir string, jobs int) *WorkerDaemon {
	cache := NewContentCache(cacheDir)
	me := &WorkerDaemon{
		secret:        secret,
		contentCache:  cache,
		mirrorMap:     make(map[string]*Mirror),
		contentServer: &ContentServer{Cache: cache},
		pending:       NewPendingConnections(),
		maxJobCount:   jobs,
		tmpDir:        tmpDir,
		rpcServer:     rpc.NewServer(),
	}
	me.cond = sync.NewCond(&me.mirrorMapMutex)
	me.stopListener = make(chan int, 1)
	me.rpcServer.Register(me)
	return me
}

const _REPORT_DELAY = 60.0

func (me *WorkerDaemon) PeriodicReport(coordinator string, port int) {
	if coordinator == "" {
		log.Println("No coordinator - not doing period reports.")
		return
	}
	me.report(coordinator, port)
	for {
		c := time.After(_REPORT_DELAY * 1e9)
		<-c
		me.report(coordinator, port)
	}
}

func (me *WorkerDaemon) report(coordinator string, port int) {
	client, err := rpc.DialHTTP("tcp", coordinator)
	if err != nil {
		log.Println("dialing coordinator:", err)
		return
	}

	hostname, err := os.Hostname()
	if err != nil {
		log.Println("hostname", err)
		return
	}

	cname, err := net.LookupCNAME(hostname)
	if err != nil {
		log.Println("cname", err)
		return
	}
	cname = strings.TrimRight(cname, ".")
	req := Registration{
		Address: fmt.Sprintf("%v:%d", cname, port),
		Name:    fmt.Sprintf("%s:%d", hostname, port),
		Version: Version(),
	}

	rep := 0
	err = client.Call("Coordinator.Register", &req, &rep)
	if err != nil {
		log.Println("coordinator rpc error:", err)
	}
}

// TODO - should expose under ContentServer name?
func (me *WorkerDaemon) FileContent(req *ContentRequest, rep *ContentResponse) os.Error {
	return me.contentServer.FileContent(req, rep)
}


type CreateMirrorRequest struct {
	RpcId        string
	RevRpcId     string
	WritableRoot string
	// Max number of processes to reserve.
	MaxJobCount int
}

type CreateMirrorResponse struct {
	GrantedJobCount int
}

func (me *WorkerDaemon) CreateMirror(req *CreateMirrorRequest, rep *CreateMirrorResponse) os.Error {
	if me.shuttingDown {
		return os.NewError("Worker is shutting down.")
	}

	rpcConn := me.pending.WaitConnection(req.RpcId)
	revConn := me.pending.WaitConnection(req.RevRpcId)
	mirror, err := me.getMirror(rpcConn, revConn, req.MaxJobCount)
	if err != nil {
		rpcConn.Close()
		revConn.Close()
		return err
	}
	mirror.writableRoot = req.WritableRoot

	rep.GrantedJobCount = mirror.maxJobCount
	return nil
}

func (me *WorkerDaemon) DropMirror(mirror *Mirror) {
	me.mirrorMapMutex.Lock()
	defer me.mirrorMapMutex.Unlock()

	log.Println("dropping mirror", mirror.key)
	me.mirrorMap[mirror.key] = nil, false
	me.cond.Broadcast()
	runtime.GC()
}

func (me *WorkerDaemon) serveConn(conn net.Conn) {
	log.Println("Authenticated connection from", conn.RemoteAddr())
	if !me.pending.Accept(conn) {
		go me.rpcServer.ServeConn(conn)
	}
}

func (me *WorkerDaemon) RunWorkerServer(port int, coordinator string) {
	out := make(chan net.Conn)

	log.Println("Worker listening to", port)

	go SetupServer(port, me.secret, out)
	go me.PeriodicReport(coordinator, port)

	for {
		select {
		case conn := <-out:
			log.Println("Authenticated connection from", conn.RemoteAddr())
			if !me.pending.Accept(conn) {
				go me.rpcServer.ServeConn(conn)
			}
		case <-me.stopListener:
			return
		}
	}
}

func (me *WorkerDaemon) Shutdown(req *int, rep *int) os.Error {
	log.Println("Received Shutdown.")

	me.mirrorMapMutex.Lock()
	defer me.mirrorMapMutex.Unlock()

	me.shuttingDown = true
	for _, m := range me.mirrorMap {
		m.Shutdown()
	}
	log.Println("Asked all mirrors to shut down.")
	for len(me.mirrorMap) > 0 {
		log.Println("Live mirror count:", len (me.mirrorMap))
		me.cond.Wait()
	}
	log.Println("All mirrors have shut down.")
	me.stopListener <- 1
	return nil
}
