package termite

import (
	"errors"
	"fmt"
	"github.com/hanwen/termite/cba"
	"github.com/hanwen/termite/stats"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"os/exec"
	"os/user"
	"strings"
	"time"
)

var _ = log.Println

type Worker struct {
	listener     net.Listener
	rpcServer    *rpc.Server
	contentCache *cba.ContentCache
	pending      *PendingConnections
	stats        *stats.ServerStats

	stopListener chan int
	mustRestart  bool
	options      *WorkerOptions
	shuttingDown bool

	mirrors *WorkerMirrors
}

type WorkerOptions struct {
	cba.ContentCacheOptions

	Paranoia bool
	Secret   []byte
	TempDir  string
	Jobs     int

	// If set, change user to this for running.
	User *user.User

	// How often to reap filesystems. If 1, use 1 FS per task.
	ReapCount int

	// Delay between contacting the coordinator for making reports.
	ReportInterval float64
	LogFileName    string
}

func NewWorker(options *WorkerOptions) *Worker {
	if options.ReapCount == 0 {
		options.ReapCount = 4
	}
	if options.ReportInterval == 0 {
		options.ReportInterval = 60.0
	}
	copied := *options

	cache := cba.NewContentCache(&options.ContentCacheOptions)

	me := &Worker{
		contentCache: cache,
		pending:      NewPendingConnections(),
		rpcServer:    rpc.NewServer(),
		stats:        stats.NewServerStats(),
		options:      &copied,
	}
	me.stats.PhaseOrder = []string{"run", "fuse", "reap"}
	me.mirrors = NewWorkerMirrors(me)
	me.stopListener = make(chan int, 1)
	me.rpcServer.Register(me)
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

	cname, err := net.LookupCNAME(Hostname)
	if err != nil {
		log.Println("cname", err)
		return
	}
	cname = strings.TrimRight(cname, ".")
	req := Registration{
		Address: fmt.Sprintf("%v:%d", cname, port),
		Name:    fmt.Sprintf("%s:%d", Hostname, port),
		Version: Version(),
	}

	rep := 0
	err = client.Call("Coordinator.Register", &req, &rep)
	if err != nil {
		log.Println("coordinator rpc error:", err)
	}
}

func (me *Worker) FileContent(req *cba.ContentRequest, rep *cba.ContentResponse) error {
	return me.contentCache.Serve(req, rep)
}

func (me *Worker) CreateMirror(req *CreateMirrorRequest, rep *CreateMirrorResponse) error {
	if me.shuttingDown {
		return errors.New("Worker is shutting down.")
	}

	rpcConn := me.pending.WaitConnection(req.RpcId)
	revConn := me.pending.WaitConnection(req.RevRpcId)
	mirror, err := me.mirrors.getMirror(rpcConn, revConn, req.MaxJobCount)
	if err != nil {
		rpcConn.Close()
		revConn.Close()
		return err
	}
	mirror.writableRoot = req.WritableRoot

	rep.GrantedJobCount = mirror.maxJobCount
	return nil
}

func (me *Worker) serveConn(conn net.Conn) {
	log.Println("Authenticated connection from", conn.RemoteAddr())
	if !me.pending.Accept(conn) {
		go me.rpcServer.ServeConn(conn)
	}
}

func (me *Worker) RunWorkerServer(port int, coordinator string) {
	me.listener = AuthenticatedListener(port, me.options.Secret)
	_, portString, _ := net.SplitHostPort(me.listener.Addr().String())

	fmt.Sscanf(portString, "%d", &port)
	go me.PeriodicReport(coordinator, port)

	for {
		conn, err := me.listener.Accept()
		if err == os.EINVAL {
			break
		}
		if err != nil {
			if e, ok := err.(*net.OpError); ok && e.Err == os.EINVAL {
				break
			}
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

func (me *Worker) Log(req *LogRequest, rep *LogResponse) error {
	if me.options.LogFileName == "" {
		return fmt.Errorf("No log filename set.")
	}

	f, err := os.Open(me.options.LogFileName)
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

func (me *Worker) DropMirror(mirror *Mirror) {
	me.mirrors.DropMirror(mirror)
}

func (me *Worker) Shutdown(req *ShutdownRequest, rep *ShutdownResponse) error {
	if req.Restart {
		me.mustRestart = true
	}
	me.shuttingDown = true
	me.mirrors.Shutdown(req)
	me.listener.Close()
	return nil
}
