package termite

import (
	"exec"
	"fmt"
	"http"
	"io"
	"io/ioutil"
	"log"
	"net"
	"os"
	"os/user"
	"rpc"
	"sync"
	"strings"
	"runtime"
	"time"
)

var _ = log.Println

type WorkerDaemon struct {
	Nobody *user.User
	secret []byte

	listener      net.Listener
	rpcServer     *rpc.Server
	contentCache  *ContentCache
	contentServer *ContentServer
	maxJobCount   int
	pending       *PendingConnections
	cacheDir      string
	tmpDir        string
	stats         *cpuStatSampler

	stopListener chan int

	mirrorMapMutex sync.Mutex
	cond           *sync.Cond
	mirrorMap      map[string]*Mirror
	shuttingDown   bool
	mustRestart    bool
	options        *WorkerOptions
}

type WorkerOptions struct {
	Secret   []byte
	TempDir  string
	CacheDir string
	Jobs     int

	// If set, change user to this for running.
	User             *string
	FileContentCount int

	// How often to reap filesystems. If 1, use 1 FS per task.
	ReapCount int

	// Delay between contacting the coordinator for making reports.
	ReportInterval float64
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

func NewWorkerDaemon(options *WorkerOptions) *WorkerDaemon {
	if options.FileContentCount == 0 {
		options.FileContentCount = 1024
	}
	if options.ReapCount == 0 {
		options.ReapCount = 4
	}
	if options.ReportInterval == 0 {
		options.ReportInterval = 60.0
	}
	copied := *options

	cache := NewContentCache(options.CacheDir)
	cache.SetMemoryCacheSize(options.FileContentCount)
	me := &WorkerDaemon{
		secret:        options.Secret,
		contentCache:  cache,
		mirrorMap:     make(map[string]*Mirror),
		contentServer: &ContentServer{Cache: cache},
		pending:       NewPendingConnections(),
		maxJobCount:   options.Jobs,
		tmpDir:        options.TempDir,
		rpcServer:     rpc.NewServer(),
		stats:         newCpuStatSampler(),
		options:       &copied,
	}
	if os.Geteuid() == 0 && options.User != nil {
		nobody, err := user.Lookup(*options.User)
		if err != nil {
			log.Fatalf("can't lookup %q: %v", options.User, err)
		}
		me.Nobody = nobody
	}
	me.cond = sync.NewCond(&me.mirrorMapMutex)
	me.stopListener = make(chan int, 1)
	me.rpcServer.Register(me)
	return me
}

func (me *WorkerDaemon) PeriodicReport(coordinator string, port int) {
	if coordinator == "" {
		log.Println("No coordinator - not doing period reports.")
		return
	}
	for !me.shuttingDown {
		me.report(coordinator, port)
		c := time.After(int64(me.options.ReportInterval * 1e9))
		<-c
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
	me.listener = AuthenticatedListener(port, me.secret)
	_, portString, _ := net.SplitHostPort(me.listener.Addr().String())

	fmt.Sscanf(portString, "%d", &port)
	go me.PeriodicReport(coordinator, port)

	for {
		conn, err := me.listener.Accept()
		if err != nil {
			log.Println("me.listener", err)
			break
		}
		log.Println("Authenticated connection from", conn.RemoteAddr())
		if !me.pending.Accept(conn) {
			go me.rpcServer.ServeConn(conn)
		}
	}

	if me.mustRestart {
		me.restart(coordinator)
	}
}

func (me *WorkerDaemon) Shutdown(req *ShutdownRequest, rep *ShutdownResponse) os.Error {
	log.Println("Received Shutdown.")
	if req.Restart {
		me.mustRestart = true
	}
	me.mirrorMapMutex.Lock()
	defer me.mirrorMapMutex.Unlock()

	me.shuttingDown = true
	for _, m := range me.mirrorMap {
		m.Shutdown()
	}
	log.Println("Asked all mirrors to shut down.")
	for len(me.mirrorMap) > 0 {
		log.Println("Live mirror count:", len(me.mirrorMap))
		me.cond.Wait()
	}
	log.Println("All mirrors have shut down.")
	me.listener.Close()
	return nil
}

func (me *WorkerDaemon) restart(coord string) {
	cl := http.Client{}
	req, err := cl.Get(fmt.Sprintf("http://%s/bin/worker", coord))
	if err != nil {
		log.Fatal("http get error.")
	}

	// We download into a tempdir, so we maintain the binary name.
	dir, err := ioutil.TempDir("", "worker-download")
	if err != nil {
		log.Fatal("TempDir:", err)
	}

	f, err := os.Create(dir + "/worker")
	if err != nil {
		log.Fatal("os.Create:", err)
	}
	io.Copy(f, req.Body)
	f.Close()
	os.Chmod(f.Name(), 0755)
	log.Println("Starting downloaded worker.")
	cmd := exec.Command(f.Name(), os.Args[1:]...)
	cmd.Start()
}
