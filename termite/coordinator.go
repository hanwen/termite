package termite

import (
	"errors"
	"fmt"
	"github.com/hanwen/termite/stats"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sort"
	"sync"
	"time"
)

var _ = log.Println

type Registration struct {
	Address string
	Name    string
	Version string
}

type Registered struct {
	Registrations []Registration
}

type WorkerRegistration struct {
	Registration
	LastReported int64
}

// Coordinator is the registration service for termite.  Workers
// register here.  A master looking for workers contacts the
// Coordinator to fetch a list of available workers.  In addition, it
// has a HTTP interface to inspect each worker.
type Coordinator struct {
	Mux *http.ServeMux

	listener net.Listener
	mutex    sync.Mutex
	workers  map[string]*WorkerRegistration
	secret   []byte
}

func NewCoordinator(secret []byte) *Coordinator {
	return &Coordinator{
		workers: make(map[string]*WorkerRegistration),
		secret:  secret,
		Mux:     http.NewServeMux(),
	}
}

func (me *Coordinator) Register(req *Registration, rep *int) error {
	conn, err := DialTypedConnection(req.Address, RPC_CHANNEL, me.secret)
	if conn != nil {
		conn.Close()
	}
	if err != nil {
		return errors.New(fmt.Sprintf(
			"error contacting address: %v", err))
	}

	me.mutex.Lock()
	defer me.mutex.Unlock()

	w := &WorkerRegistration{*req, 0}
	w.LastReported = time.Seconds()
	me.workers[w.Address] = w
	return nil
}

func (me *Coordinator) WorkerCount() int {
	me.mutex.Lock()
	defer me.mutex.Unlock()
	return len(me.workers)
}

func (me *Coordinator) List(req *int, rep *Registered) error {
	me.mutex.Lock()
	defer me.mutex.Unlock()

	keys := []string{}
	for k := range me.workers {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		w := me.workers[k]
		rep.Registrations = append(rep.Registrations, w.Registration)
	}
	return nil
}

func (me *Coordinator) checkReachable() {
	now := time.Seconds()

	addrs := me.workerAddresses()

	var toDelete []string
	for _, a := range addrs {
		conn, err := DialTypedConnection(a, RPC_CHANNEL, me.secret)
		if err != nil {
			toDelete = append(toDelete, a)
		} else {
			conn.Close()
		}
	}

	me.mutex.Lock()
	for _, a := range toDelete {
		if me.workers[a].LastReported < now {
			delete(me.workers, a)
		}
	}
	me.mutex.Unlock()
}

const _POLL = 60

func (me *Coordinator) PeriodicCheck() {
	for {
		c := time.After(_POLL * 1e9)
		<-c
		me.checkReachable()
	}
}

func (me *Coordinator) rootHandler(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "text/html")
	me.mutex.Lock()
	defer me.mutex.Unlock()

	fmt.Fprintf(w, "<html><head><title>Termite coordinator</title></head>")
	fmt.Fprintf(w, "<body><h1>Termite coordinator</h1><ul>")
	fmt.Fprintf(w, "<p>version %s", Version())
	defer fmt.Fprintf(w, "</body></html>")

	keys := []string{}
	for k := range me.workers {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, k := range keys {
		worker := me.workers[k]
		fmt.Fprintf(w, "<li><a href=\"worker?host=%s\">address <tt>%s</tt>, host <tt>%s</tt></a>",
			worker.Address, worker.Address, worker.Name)
	}
	fmt.Fprintf(w, "</ul>")

	fmt.Fprintf(w, "<hr><p><a href=\"killall\">kill all workers,</a>"+
		"<a href=\"restartall\">restart all workers</a>")
}

func (me *Coordinator) shutdownSelf(w http.ResponseWriter, req *http.Request) {
	fmt.Fprintf(w, "<html><head><title>Termite coordinator</title></head>")
	fmt.Fprintf(w, "<body><h1>Shutdown in progress</h1><ul>")

	// Async, so we can still return the reply here.
	time.AfterFunc(100e6, func() { me.Shutdown() })
}

func (me *Coordinator) killAllHandler(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "text/html")
	restart := req.URL.Path == "restartall"
	errs := []error{}
	for _, w := range me.workerAddresses() {
		conn, err := DialTypedConnection(w, RPC_CHANNEL, me.secret)
		if err == nil {
			killReq := ShutdownRequest{Restart: restart}
			rep := ShutdownResponse{}
			cl := rpc.NewClient(conn)
			defer cl.Close()
			err = cl.Call("Worker.Shutdown", &killReq, &rep)
		}

		if err != nil {
			errs = append(errs, fmt.Errorf("%s: %v", w, err))
		}
	}

	if len(errs) > 0 {
		fmt.Fprintf(w, "Error: %v", errs)
		return
	}

	action := "kill"
	if restart {
		action = "restart"
	}
	fmt.Fprintf(w, "<p>%s in progress", action)
	// Should have a redirect.
	fmt.Fprintf(w, "<p><a href=\"/\">back to index</a>")
	go me.checkReachable()
}

func (me *Coordinator) killHandler(w http.ResponseWriter, req *http.Request) {
	addr, conn, err := me.getHost(req)
	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w, "<html><head><title>Termite worker error</title></head>")
		fmt.Fprintf(w, "<body>Error: %s</body></html>", err.Error())
		return
	}
	defer conn.Close()

	w.Header().Set("Content-Type", "text/html")
	restart := req.URL.Path == "restart"
	fmt.Fprintf(w, "<html><head><title>Termite worker status</title></head>")
	fmt.Fprintf(w, "<body><h1>Status %s</h1>", addr)
	defer fmt.Fprintf(w, "</body></html>")

	killReq := ShutdownRequest{Restart: restart}
	rep := ShutdownResponse{}
	cl := rpc.NewClient(conn)
	defer cl.Close()
	err = cl.Call("Worker.Shutdown", &killReq, &rep)
	if err != nil {
		fmt.Fprintf(w, "<p><tt>Error: %v<tt>", err)
		return
	}

	action := "kill"
	if restart {
		action = "restart"
	}
	fmt.Fprintf(w, "<p>%s of %s in progress", action, conn.RemoteAddr())
	// Should have a redirect.
	fmt.Fprintf(w, "<p><a href=\"/\">back to index</a>")
	go me.checkReachable()
}

func (me *Coordinator) shutdownWorker(addr string, restart bool) error {
	conn, err := DialTypedConnection(addr, RPC_CHANNEL, me.secret)
	if err != nil {
		return err
	}

	killReq := ShutdownRequest{Restart: restart}
	rep := ShutdownResponse{}
	cl := rpc.NewClient(conn)
	err = cl.Call("Worker.Shutdown", &killReq, &rep)
	cl.Close()
	conn.Close()
	return err
}

func (me *Coordinator) workerAddresses() (out []string) {
	me.mutex.Lock()
	defer me.mutex.Unlock()
	for k := range me.workers {
		out = append(out, k)
	}
	return out
}

func (me *Coordinator) haveWorker(addr string) bool {
	me.mutex.Lock()
	defer me.mutex.Unlock()
	_, ok := me.workers[addr]
	return ok
}

func (me *Coordinator) getHost(req *http.Request) (string, net.Conn, error) {
	q := req.URL.Query()
	vs, ok := q["host"]
	if !ok || len(vs) == 0 {
		return "", nil, fmt.Errorf("query param 'host' missing")
	}
	addr := string(vs[0])
	if !me.haveWorker(addr) {
		return "", nil, fmt.Errorf("worker %q unknown", addr)
	}

	conn, err := DialTypedConnection(addr, RPC_CHANNEL, me.secret)
	if err != nil {
		return "", nil, fmt.Errorf("error dialing: %v", err)
	}

	return addr, conn, nil
}

func (me *Coordinator) logHandler(w http.ResponseWriter, req *http.Request) {
	_, conn, err := me.getHost(req)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "<html><head><title>Termite worker error</title></head>")
		fmt.Fprintf(w, "<body>Error: %s</body></html>", err.Error())
		return
	}
	sz := int64(500 * 1024)
	sizeStr, ok := req.URL.Query()["size"]
	if ok {
		fmt.Scanf(sizeStr[0], "%d", &sz)
	}

	logReq := LogRequest{Whence: os.SEEK_END, Off: -sz, Size: sz}
	logRep := LogResponse{}
	client := rpc.NewClient(conn)
	err = client.Call("Worker.Log", &logReq, &logRep)
	client.Close()
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "<html><head><title>Termite worker error</title></head>")
		fmt.Fprintf(w, "<body>Error: %s</body></html>", err.Error())
		return
	}

	w.Header().Set("Content-Type", "text/plain")
	w.Write(logRep.Data)
	return
}

func (me *Coordinator) workerHandler(w http.ResponseWriter, req *http.Request) {
	addr, conn, err := me.getHost(req)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "<html><head><title>Termite worker error</title></head>")
		fmt.Fprintf(w, "<body>Error: %s</body></html>", err.Error())
		return
	}

	statusReq := WorkerStatusRequest{}
	status := WorkerStatusResponse{}

	client := rpc.NewClient(conn)
	err = client.Call("Worker.Status", &statusReq, &status)
	client.Close()
	if err != nil {
		fmt.Fprintf(w, "<p><tt>RPC error: %v<tt>\n", err)
		return
	}

	fmt.Fprintf(w, "<p>Worker %s<p>Version %s<p>Jobs %d\n",
		addr, status.Version, status.MaxJobCount)
	fmt.Fprintf(w, "<p><a href=\"/log?host=%s\">Worker log %s</a>\n", addr, addr)

	stats.CpuStatsWriteHttp(w, status.CpuStats)

	fmt.Fprintf(w, "<p>Total CPU: %s", status.TotalCpu.Percent())
	fmt.Fprintf(w, "<p>Content cache hit rate: %.0f %%", 100.0*status.ContentCacheHitRate)

	fmt.Fprintf(w, "<ul>\n")
	for i, n := range status.PhaseNames {
		fmt.Fprintf(w, "<li>jobs in phase %s: %d\n", n, status.PhaseCounts[i])
	}
	fmt.Fprintf(w, "</ul>\n")

	for _, mirrorStatus := range status.MirrorStatus {
		me.mirrorStatusHtml(w, mirrorStatus)
	}
	fmt.Fprintf(w, "<p><a href=\"/workerkill?host=%s\">Kill worker %s</a>\n", addr, addr)
	fmt.Fprintf(w, "<p><a href=\"/restart?host=%s\">Restart worker %s</a>\n", addr, addr)
	conn.Close()
}

func (me *Coordinator) mirrorStatusHtml(w http.ResponseWriter, s MirrorStatusResponse) {
	fmt.Fprintf(w, "<h2>Mirror %s</h2>", s.Root)
	for _, s := range s.RpcTimings {
		fmt.Fprintf(w, "<li>%s", s)
	}

	fmt.Fprintf(w, "<p>%d maximum jobs, %d running, %d waiting tasks, %d unused filesystems.\n",
		s.Granted, len(s.Running), s.WaitingTasks, s.IdleFses)
	if s.ShuttingDown {
		fmt.Fprintf(w, "<p><b>shutting down</b>\n")
	}

	fmt.Fprintf(w, "<ul>\n")
	for _, v := range s.Running {
		fmt.Fprintf(w, "<li>%s\n", v)
	}
	fmt.Fprintf(w, "</ul>\n")
}

func (me *Coordinator) Shutdown() {
	log.Println("Coordinator shutdown.")
	me.listener.Close()
}

func (me *Coordinator) ServeHTTP(port int) {
	me.Mux.HandleFunc("/",
		func(w http.ResponseWriter, req *http.Request) {
			me.rootHandler(w, req)
		})
	me.Mux.HandleFunc("/worker",
		func(w http.ResponseWriter, req *http.Request) {
			me.workerHandler(w, req)
		})
	me.Mux.HandleFunc("/log",
		func(w http.ResponseWriter, req *http.Request) {
			me.logHandler(w, req)
		})
	me.Mux.HandleFunc("/shutdown",
		func(w http.ResponseWriter, req *http.Request) {
			me.shutdownSelf(w, req)
		})
	me.Mux.HandleFunc("/workerkill",
		func(w http.ResponseWriter, req *http.Request) {
			me.killHandler(w, req)
		})
	me.Mux.HandleFunc("/killall",
		func(w http.ResponseWriter, req *http.Request) {
			me.killAllHandler(w, req)
		})
	me.Mux.HandleFunc("/restartall",
		func(w http.ResponseWriter, req *http.Request) {
			me.killAllHandler(w, req)
		})
	me.Mux.HandleFunc("/restart",
		func(w http.ResponseWriter, req *http.Request) {
			me.killHandler(w, req)
		})

	rpcServer := rpc.NewServer()
	if err := rpcServer.Register(me); err != nil {
		log.Fatal("rpcServer.Register:", err)
	}
	me.Mux.HandleFunc(rpc.DefaultRPCPath,
		func(w http.ResponseWriter, req *http.Request) {
			rpcServer.ServeHTTP(w, req)
		})

	addr := fmt.Sprintf(":%d", port)
	var err error
	me.listener, err = net.Listen("tcp", addr)
	if err != nil {
		log.Fatal("net.Listen: ", err.Error())
	}
	log.Println("Coordinator listening on", addr)

	httpServer := http.Server{
		Addr:    addr,
		Handler: me.Mux,
	}
	err = httpServer.Serve(me.listener)
	if err != nil {
		log.Println("httpServer.Serve: ", err)
	}
}
