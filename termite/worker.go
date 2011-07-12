package termite

import (
	"fmt"
	"log"
	"net"
	"os"
	"rpc"
	"sync"
	"time"
)

var _ = log.Println

type WorkReply struct {
	Exit   *os.Waitmsg
	Files  []AttrResponse
	Stderr string
	Stdout string
}

type WorkRequest struct {
	// Id of connection streaming stdin.
	StdinId      string
	Debug        bool
	WritableRoot string
	Binary       string
	Argv         []string
	Env          []string
	Dir          string
}

func (me *WorkRequest) Summary() string {
	return fmt.Sprintf("stdin %s cmd %s", me.StdinId, me.Argv)
}

type WorkerDaemon struct {
	secret       []byte
	ChrootBinary string

	contentCache  *DiskFileCache
	contentServer *ContentServer
	maxJobCount   int
	pending       *PendingConnections
	cacheDir      string
	tmpDir        string
	// TODO - deal with closed connections.
	mirrorMapMutex sync.Mutex
	mirrorMap      map[string]*Mirror
}

func (me *WorkerDaemon) getMirror(rpcConn, revConn net.Conn, reserveCount int) (*Mirror, os.Error) {
	me.mirrorMapMutex.Lock()
	defer me.mirrorMapMutex.Unlock()
	used := 0
	for _, v := range me.mirrorMap {
		used += v.maxJobCount
	}
	if reserveCount <= 0 {
		return nil, os.NewError("must ask positive jobcount")
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
	cache := NewDiskFileCache(cacheDir)
	w := &WorkerDaemon{
		secret:        secret,
		contentCache:  cache,
		mirrorMap:     make(map[string]*Mirror),
		contentServer: &ContentServer{Cache: cache},
		pending:       NewPendingConnections(),
		maxJobCount:   jobs,
		tmpDir:        tmpDir,
	}
	return w
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

	req := Registration{
		Address: fmt.Sprintf("%s:%d", hostname, port), // TODO - resolve.
		Name:fmt.Sprintf("%s:%d", hostname, port),
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

func trim(s string) string {
	l := 1024
	if len(s) < l {
		l = len(s)
	}
	return s[:l]
}

type CreateMirrorRequest struct {
	RpcId        string
	RevRpcId     string
	WritableRoot string
	// Max number of processes to reserve.
	MaxJobCount int
}

type CreateMirrorResponse struct {
	MaxJobCount int
}

func (me *WorkerDaemon) CreateMirror(req *CreateMirrorRequest, rep *CreateMirrorResponse) os.Error {
	log.Println("CreateMirror")
	rpcConn := me.pending.WaitConnection(req.RpcId)
	revConn := me.pending.WaitConnection(req.RevRpcId)
	mirror, err := me.getMirror(rpcConn, revConn, req.MaxJobCount)
	if err != nil {
		return err
	}
	mirror.writableRoot = req.WritableRoot

	rep.MaxJobCount = mirror.maxJobCount
	return nil
}
func (me *WorkerDaemon) DropMirror(mirror *Mirror) {
	me.mirrorMapMutex.Lock()
	defer me.mirrorMapMutex.Unlock()

	log.Println("dropping mirror", mirror.key)
	me.mirrorMap[mirror.key] = nil, false
}

func (me *WorkerDaemon) RunWorkerServer(port int, coordinator string) {
	out := make(chan net.Conn)
	go SetupServer(port, me.secret, out)
	go me.PeriodicReport(coordinator, port)

	for {
		conn := <-out
		log.Println("Authenticated connection from", conn.RemoteAddr())
		if !me.pending.Accept(conn) {
			rpcServer := rpc.NewServer()
			rpcServer.Register(me)
			go rpcServer.ServeConn(conn)
		}
	}
}

type StatusRequest struct {

}

type StatusReply struct {
	Processes int
}

func (me *WorkerDaemon) Status(req *StatusRequest, rep *StatusReply) os.Error {
	me.mirrorMapMutex.Lock()
	defer me.mirrorMapMutex.Unlock()
	for _, mirror := range me.mirrorMap {
		mirror.Status(req, rep)
	}

	// Always return nil, so we know any errors are due to connection problems.
	return nil
}
