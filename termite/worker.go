package termite

import (
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"os/exec"
	"os/user"
	"runtime"
	"strings"
	"sync"
	"time"
)

var _ = log.Println

type Worker struct {
	Nobody       *user.User
	secret       []byte
	Hostname     string
	listener     net.Listener
	rpcServer    *rpc.Server
	contentCache *ContentCache
	maxJobCount  int
	pending      *PendingConnections
	cacheDir     string
	tmpDir       string
	stats        *cpuStatSampler

	stopListener chan int

	mirrorMapMutex sync.Mutex
	cond           *sync.Cond
	mirrorMap      map[string]*Mirror
	shuttingDown   bool
	mustRestart    bool
	options        *WorkerOptions
	LogFileName    string
}

type WorkerOptions struct {
	Paranoia bool
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

func (me *Worker) getMirror(rpcConn, revConn net.Conn, reserveCount int) (*Mirror, error) {
	if reserveCount <= 0 {
		return nil, errors.New("must ask positive jobcount")
	}
	me.mirrorMapMutex.Lock()
	defer me.mirrorMapMutex.Unlock()
	used := 0
	for _, v := range me.mirrorMap {
		used += v.maxJobCount
	}

	remaining := me.maxJobCount - used
	if remaining <= 0 {
		return nil, errors.New("no processes available")
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

func NewWorker(options *WorkerOptions) *Worker {
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
	me := &Worker{
		secret:       options.Secret,
		contentCache: cache,
		mirrorMap:    make(map[string]*Mirror),
		pending:      NewPendingConnections(),
		maxJobCount:  options.Jobs,
		tmpDir:       options.TempDir,
		rpcServer:    rpc.NewServer(),
		stats:        newCpuStatSampler(),
		options:      &copied,
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
	me.Hostname, _ = os.Hostname()
	return me
}

func (me *Worker) PeriodicReport(coordinator string, port int) {
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

func (me *Worker) report(coordinator string, port int) {
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
func (me *Worker) FileContent(req *ContentRequest, rep *ContentResponse) error {
	return ServeFileContent(me.contentCache, req, rep)
}

func (me *Worker) CreateMirror(req *CreateMirrorRequest, rep *CreateMirrorResponse) error {
	if me.shuttingDown {
		return errors.New("Worker is shutting down.")
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

func (me *Worker) DropMirror(mirror *Mirror) {
	me.mirrorMapMutex.Lock()
	defer me.mirrorMapMutex.Unlock()

	log.Println("dropping mirror", mirror.key)
	delete(me.mirrorMap, mirror.key)
	me.cond.Broadcast()
	runtime.GC()
}

func (me *Worker) serveConn(conn net.Conn) {
	log.Println("Authenticated connection from", conn.RemoteAddr())
	if !me.pending.Accept(conn) {
		go me.rpcServer.ServeConn(conn)
	}
}

func (me *Worker) RunWorkerServer(port int, coordinator string) {
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

func (me *Worker) Shutdown(req *ShutdownRequest, rep *ShutdownResponse) error {
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

func (me *Worker) Log(req *LogRequest, rep *LogResponse) error {
	if me.LogFileName == "" {
		return fmt.Errorf("No log filename set.")
	}

	f, err := os.Open(me.LogFileName)
	if err != nil {
		return err
	}
	defer f.Close()

	size, err := f.Seek(0, os.SEEK_END)
	if err != nil {
		return err
	}

	switch req.Whence {
	case os.SEEK_END:
		if req.Off < -size {
			req.Off = -size
		}
		if req.Off+req.Size > 0 {
			req.Size = -req.Off
		}
	case os.SEEK_SET:
		if req.Off > size {
			req.Off = size
		}
		if req.Off+req.Size > size {
			req.Size = size - req.Off
		}
	}

	log.Printf("Sending log: %v", req)
	_, err = f.Seek(req.Off, req.Whence)
	if err != nil {
		return err
	}
	rep.Data = make([]byte, req.Size)
	n, err := f.Read(rep.Data)
	if err != nil {
		return err
	}
	rep.Data = rep.Data[:n]
	return nil
}

func (me *Worker) restart(coord string) {
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
