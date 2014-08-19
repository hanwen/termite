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
	"time"

	"github.com/hanwen/termite/cba"
	"github.com/hanwen/termite/stats"
)

type Worker struct {
	listener  connListener
	rpcServer *rpc.Server
	content   *cba.Store
	stats     *stats.ServerStats

	stopListener   chan int
	canRestart     bool
	options        *WorkerOptions
	accepting      bool
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

	Secret  []byte
	TempDir string
	Jobs    int

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

	// full path to mkbox binary
	Mkbox string
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

	w := &Worker{
		content:    cache,
		rpcServer:  rpc.NewServer(),
		stats:      stats.NewServerStats(),
		options:    &copied,
		accepting:  true,
		canRestart: true,
	}
	w.stats.PhaseOrder = []string{"run", "fuse", "reap"}
	w.mirrors = NewWorkerMirrors(w)
	w.stopListener = make(chan int, 1)
	w.rpcServer.Register(w)

	return w
}

func (w *Worker) PeriodicHouseholding() {
	for w.accepting {
		w.Report()
		if w.options.HeapLimit > 0 {
			heap := stats.GetMemStat().Total()
			if heap > w.options.HeapLimit {
				log.Println("Exceeded heap limit. Restarting...")
				w.shutdown(true, false)
			}
		}

		c := time.After(w.options.ReportInterval)
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

func (w *Worker) Report() {
	if w.options.Coordinator == "" {
		return
	}
	client, err := rpc.DialHTTP("tcp", w.options.Coordinator)
	if err != nil {
		log.Println("dialing coordinator:", err)
		return
	}

	req := RegistrationRequest{
		Address:        fmt.Sprintf("%v:%d", cname, w.options.Port),
		Name:           fmt.Sprintf("%s:%d", Hostname, w.options.Port),
		Version:        Version(),
		HttpStatusPort: w.httpStatusPort,
	}
	rep := Empty{}
	err = client.Call("Coordinator.Register", &req, &rep)
	if err != nil {
		log.Println("coordinator rpc error:", err)
	}
}

func (w *Worker) CreateMirror(req *CreateMirrorRequest, rep *CreateMirrorResponse) error {
	if !w.accepting {
		return errors.New("Worker is shutting down.")
	}
	rpcConn := w.listener.Accept(req.RpcId)
	revConn := w.listener.Accept(req.RevRpcId)
	contentConn := w.listener.Accept(req.ContentId)
	revContentConn := w.listener.Accept(req.RevContentId)
	mirror, err := w.mirrors.getMirror(rpcConn, revConn, contentConn, revContentConn, req.MaxJobCount, req.WritableRoot)
	if err != nil {
		rpcConn.Close()
		revConn.Close()
		contentConn.Close()
		revContentConn.Close()
		return err
	}

	rep.GrantedJobCount = mirror.maxJobCount
	return nil
}

func (w *Worker) RunWorkerServer() {
	listener := portRangeListener(w.options.Port, w.options.PortRetry)

	_, portString, _ := net.SplitHostPort(listener.Addr().String())
	fmt.Sscanf(portString, "%d", &w.options.Port)
	go w.PeriodicHouseholding()
	go w.serveStatus(w.options.Port, w.options.PortRetry)

	incomingRpc := make(chan io.ReadWriteCloser, 1)
	go func() {
		for c := range incomingRpc {
			go w.rpcServer.ServeConn(c)
		}
	}()

	w.listener = newTCPListener(listener, w.options.Secret, incomingRpc)
	w.listener.Wait()
}

func (w *Worker) Log(req *LogRequest, rep *LogResponse) error {
	if w.options.LogFileName == "" {
		return fmt.Errorf("No log filename set.")
	}

	f, err := os.Open(w.options.LogFileName)
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

func (w *Worker) restart() {
	cl := http.Client{}
	req, err := cl.Get(fmt.Sprintf("http://%s/bin/worker", w.options.Coordinator))
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

func (w *Worker) DropMirror(mirror *Mirror) {
	w.mirrors.DropMirror(mirror)
}

func (w *Worker) Shutdown(req *ShutdownRequest, rep *ShutdownResponse) error {
	log.Printf("Received Shutdown RPC: %#v", req)
	w.shutdown(req.Restart, req.Kill)
	return nil
}

func (w *Worker) shutdown(restart bool, aggressive bool) {
	if restart && w.canRestart {
		w.canRestart = false
		w.restart()

		// Wait a bit, since we don't want to shutdown before
		// the new worker is up
		time.Sleep(2 * time.Second)
	}
	w.accepting = false
	go func() {
		w.mirrors.shutdown(aggressive)

		// Sleep to give the master some time to process the results.
		if !aggressive {
			time.Sleep(w.options.LameDuckPeriod)
		}
		w.listener.Close()
	}()
}
