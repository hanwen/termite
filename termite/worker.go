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
	"strings"
	"syscall"
	"time"
	
	"github.com/hanwen/termite/cba"
	"github.com/hanwen/termite/stats"
)

var _ = log.Println

type Worker struct {
	listener  net.Listener
	rpcServer *rpc.Server
	content   *cba.Store
	pending   *PendingConnections
	stats     *stats.ServerStats

	stopListener chan int
	canRestart   bool
	options      *WorkerOptions
	accepting    bool

	httpStatusPort int
	mirrors        *WorkerMirrors
}

type User struct {
	Uid int
	Gid int
}

type WorkerOptions struct {
	cba.StoreOptions

	// Address of the coordinator.
	Coordinator string

	// (starting) port to listen to.
	Port int

	// How many other ports try.
	PortRetry int

	Paranoia bool
	Secret   []byte
	TempDir  string
	Jobs     int

	// If set, change user to this for running.
	User *User

	// How often to reap filesystems. If 1, use 1 FS per task.
	ReapCount int

	// Delay between contacting the coordinator for making reports.
	ReportInterval time.Duration
	LogFileName    string

	// If set, we restart once the heap usage passes this
	// threshold.
	HeapLimit uint64

	// How long to wait between the last task exit, and shutting
	// down the server.
	LameDuckPeriod time.Duration
}

func NewWorker(options *WorkerOptions) *Worker {
	copied := *options
	options = &copied
	if options.ReapCount == 0 {
		options.ReapCount = 4
	}
	if options.ReportInterval == 0 {
		options.ReportInterval = 60 * time.Second
	}
	if options.LameDuckPeriod == 0 {
		options.LameDuckPeriod = 5 * time.Second
	}

	if fi, _ := os.Stat(options.TempDir); fi == nil || !fi.IsDir() {
		log.Fatalf("directory %s does not exist, or is not a dir", options.TempDir)
	}
	// TODO - check that we can do renames from temp to cache.

	cache := cba.NewStore(&options.StoreOptions)

	me := &Worker{
		content:    cache,
		pending:    NewPendingConnections(),
		rpcServer:  rpc.NewServer(),
		stats:      stats.NewServerStats(),
		options:    &copied,
		accepting:  true,
		canRestart: true,
	}
	me.stats.PhaseOrder = []string{"run", "fuse", "reap"}
	me.mirrors = NewWorkerMirrors(me)
	me.stopListener = make(chan int, 1)
	me.rpcServer.Register(me)
	return me
}

func (me *Worker) PeriodicHouseholding() {
	for me.accepting {
		me.Report()
		if me.options.HeapLimit > 0 {
			heap := stats.GetMemStat().Total()
			if heap > me.options.HeapLimit {
				log.Println("Exceeded heap limit. Restarting...")
				me.shutdown(true, false)
			}
		}

		c := time.After(me.options.ReportInterval)
		<-c
	}
}

var cname string

func init() {
	var err error
	cname, err = net.LookupCNAME(Hostname)
	if err != nil {
		log.Println("cname", err)
		return
	}
	cname = strings.TrimRight(cname, ".")
}

func (me *Worker) Report() {
	if me.options.Coordinator == "" {
		return
	}
	client, err := rpc.DialHTTP("tcp", me.options.Coordinator)
	if err != nil {
		log.Println("dialing coordinator:", err)
		return
	}

	req := RegistrationRequest{
		Address:        fmt.Sprintf("%v:%d", cname, me.options.Port),
		Name:           fmt.Sprintf("%s:%d", Hostname, me.options.Port),
		Version:        Version(),
		HttpStatusPort: me.httpStatusPort,
	}
	rep := Empty{}
	err = client.Call("Coordinator.Register", &req, &rep)
	if err != nil {
		log.Println("coordinator rpc error:", err)
	}
}

func (me *Worker) CreateMirror(req *CreateMirrorRequest, rep *CreateMirrorResponse) error {
	if !me.accepting {
		return errors.New("Worker is shutting down.")
	}

	rpcConn := me.pending.WaitConnection(req.RpcId)
	revConn := me.pending.WaitConnection(req.RevRpcId)
	contentConn := me.pending.WaitConnection(req.ContentId)
	revContentConn := me.pending.WaitConnection(req.RevContentId)
	mirror, err := me.mirrors.getMirror(rpcConn, revConn, contentConn, revContentConn, req.MaxJobCount)
	if err != nil {
		rpcConn.Close()
		revConn.Close()
		contentConn.Close()
		revContentConn.Close()
		return err
	}
	mirror.writableRoot = req.WritableRoot

	rep.GrantedJobCount = mirror.maxJobCount
	return nil
}

func (me *Worker) RunWorkerServer() {
	me.listener = AuthenticatedListener(me.options.Port, me.options.Secret, me.options.PortRetry)
	_, portString, _ := net.SplitHostPort(me.listener.Addr().String())
	fmt.Sscanf(portString, "%d", &me.options.Port)
	go me.PeriodicHouseholding()
	go me.serveStatus()

	for {
		conn, err := me.listener.Accept()
		if err == syscall.EINVAL {
			break
		}
		if err != nil {
			if e, ok := err.(*net.OpError); ok && e.Err == syscall.EINVAL {
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

func (me *Worker) restart() {
	cl := http.Client{}
	req, err := cl.Get(fmt.Sprintf("http://%s/bin/worker", me.options.Coordinator))
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
	log.Printf("Received Shutdown RPC: %#v", req)
	me.shutdown(req.Restart, req.Kill)
	return nil
}

func (me *Worker) shutdown(restart bool, aggressive bool) {
	if restart && me.canRestart {
		me.canRestart = false
		me.restart()

		// Wait a bit, since we don't want to shutdown before
		// the new worker is up
		time.Sleep(2 * time.Second)
	}
	me.accepting = false
	go func() {
		me.mirrors.shutdown(aggressive)

		// Sleep to give the master some time to process the results.
		if !aggressive {
			time.Sleep(me.options.LameDuckPeriod)
		}
		me.listener.Close()
	}()
}
