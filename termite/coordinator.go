package termite

import (
	"fmt"
	"http"
	"log"
	"net"
	"os"
	"rpc"
	"sync"
	"time"
)

var _ = log.Println

type Registration struct {
	Address string
	Name    string
	Version string
	// TODO - hash of the secret?
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

func (me *Coordinator) Register(req *Registration, rep *int) os.Error {
	conn, err := DialTypedConnection(req.Address, RPC_CHANNEL, me.secret)
	if conn != nil {
		conn.Close()
	}
	if err != nil {
		return os.NewError(fmt.Sprintf(
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

func (me *Coordinator) List(req *int, rep *Registered) os.Error {
	me.mutex.Lock()
	defer me.mutex.Unlock()

	for _, w := range me.workers {
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
			me.workers[a] = nil, false
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
	defer fmt.Fprintf(w, "</body></html>")

	for _, worker := range me.workers {
		fmt.Fprintf(w, "<li><a href=\"worker?host=%s\">address <tt>%s</tt>, host <tt>%s</tt></a>",
			worker.Address, worker.Address, worker.Name)
	}
	fmt.Fprintf(w, "</ul>")

	fmt.Fprintf(w, "<hr><p><a href=\"workerkill?host=all\">kill all workers,</a>" +
		"<a href=\"restart?host=all\">restart all workers</a>")
}

func (me *Coordinator) shutdownSelf(w http.ResponseWriter, req *http.Request) {
	fmt.Fprintf(w, "<html><head><title>Termite coordinator</title></head>")
	fmt.Fprintf(w, "<body><h1>Shutdown in progress</h1><ul>")

	// Async, so we can still return the reply here.
	time.AfterFunc(100e6, func() { me.Shutdown() }())
}
	

func (me *Coordinator) killHandler(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "text/html")
	q := req.URL.Query()
	vs, ok := q["host"]
	if !ok || len(vs) == 0 {
		fmt.Fprintf(w, "<html><body>404 query param 'host' missing</body></html>")
		return
	}
	addr := string(vs[0])

	restart := req.URL.Path == "restart"
	fmt.Fprintf(w, "<html><head><title>Termite worker status</title></head>")
	fmt.Fprintf(w, "<body><h1>Status %s</h1>", addr)
	defer fmt.Fprintf(w, "</body></html>")

	if addr != "all" && !me.haveWorker(addr) {
		fmt.Fprintf(w, "<p><tt>worker %q unknown<tt>", addr)
		return
	}

	var err os.Error
	if addr == "all" {
		errs := []os.Error{}
		for _, w := range me.workerAddresses() {
			err = me.shutdownWorker(w, restart)
			if err != nil {
				errs = append(errs, fmt.Errorf("%s: %v", w, err))
			}
		}
		if len(errs) > 0 {
			err = fmt.Errorf("%v", errs)
		}
	} else {
		err = me.shutdownWorker(addr, restart)
	}
	
	if err != nil {
		fmt.Fprintf(w, "<p><tt>Error: %v<tt>", err)
		return
	}
	
	action := "kill"
	if restart {
		action = "restart"
	}
	fmt.Fprintf(w, "<p>%s of %s in progress", action, addr)
	// Should have a redirect.
	fmt.Fprintf(w, "<p><a href=\"/\">back to index</a>")
	go me.checkReachable()
}

func (me *Coordinator) shutdownWorker(addr string, restart bool) os.Error {
	conn, err := DialTypedConnection(addr, RPC_CHANNEL, me.secret)
	if err != nil {
		return err
	}

	killReq := ShutdownRequest{Restart: restart}
	rep := ShutdownResponse{}
	cl := rpc.NewClient(conn)
	err = cl.Call("WorkerDaemon.Shutdown", &killReq, &rep)
	cl.Close()
	conn.Close()
	return err
}

func (me *Coordinator) workerAddresses() (out []string) {
	me.mutex.Lock()
	defer me.mutex.Unlock()
	for k, _ := range me.workers {
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

func (me *Coordinator) workerHandler(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "text/html")
	q := req.URL.Query()
	vs, ok := q["host"]
	if !ok || len(vs) == 0 {
		fmt.Fprintf(w, "<html><body>404 query param 'host' missing</body></html>")
		return
	}
	addr := string(vs[0])
	fmt.Fprintf(w, "<html><head><title>Termite worker status</title></head>")
	fmt.Fprintf(w, "<body><h1>Status %s</h1>", addr)
	defer fmt.Fprintf(w, "</body></html>")

	if !me.haveWorker(addr) {
		fmt.Fprintf(w, "<p><tt>worker %q unknown<tt>", addr)
		return
	}

	conn, err := DialTypedConnection(addr, RPC_CHANNEL, me.secret)
	if err != nil {
		fmt.Fprintf(w, "<p><tt>error dialing: %v<tt>", err)
		return
	}

	statusReq := WorkerStatusRequest{}
	status := WorkerStatusResponse{}

	client := rpc.NewClient(conn)
	err = client.Call("WorkerDaemon.Status", &statusReq, &status)
	client.Close()
	if err != nil {
		fmt.Fprintf(w, "<p><tt>RPC error: %v<tt>\n", err)
		return
	}

	fmt.Fprintf(w, "<p>Worker %s<p>Version %s<p>Jobs %d\n",
		addr, status.Version, status.MaxJobCount)
	fmt.Fprintf(w, "<p><table><tr><th>self cpu (ms)</th><th>self sys (ms)</th>"+
		"<th>child cpu (ms)</th><th>child sys (ms)</th><th>total</th></tr>")
	start := len(status.CpuStats)-5
	if start < 0 {
		start = 0
	}
	
	for _, v := range status.CpuStats[start:] {
		fmt.Fprintf(w, "<tr><td>%d</td><td>%d</td><td>%d</td><td>%d</td><td>%d</td></tr>",
			v.SelfCpu/1e6, v.SelfSys/1e6, v.ChildCpu/1e6, v.ChildSys/1e6,
			(v.SelfCpu+v.SelfSys+v.ChildCpu+v.ChildSys)/1e6)
	}
	fmt.Fprintf(w, "</table>")

	minuteStat := CpuStat{}
	for _, v := range status.CpuStats {
		minuteStat = minuteStat.Add(v)
	}

	fmt.Fprintf(w, "<p>Last minute: %s (%.1f CPUs.)", minuteStat.Percent(),
		float64(minuteStat.Total()) / 60.0e9)
	
	fmt.Fprintf(w, "<p>Total CPU: %s", status.TotalCpu.Percent())

	for _, mirrorStatus := range status.MirrorStatus {
		me.mirrorStatusHtml(w, mirrorStatus)
	}
	fmt.Fprintf(w, "<p><a href=\"/workerkill?host=%s\">Kill worker %s</a>\n", addr, addr)
	fmt.Fprintf(w, "<p><a href=\"/restart?host=%s\">Restart worker %s</a>\n", addr, addr)
	conn.Close()
}

func (me *Coordinator) mirrorStatusHtml(w http.ResponseWriter, s MirrorStatusResponse) {
	fmt.Fprintf(w, "<h2>Mirror %s</h2>", s.Root)
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
	me.Mux.HandleFunc("/shutdown",
		func(w http.ResponseWriter, req *http.Request) {
			me.shutdownSelf(w, req)
		})
	me.Mux.HandleFunc("/workerkill",
		func(w http.ResponseWriter, req *http.Request) {
			me.killHandler(w, req)
		})
	me.Mux.HandleFunc("/restart",
		func(w http.ResponseWriter, req *http.Request) {
			me.killHandler(w, req)
		})

	rpcServer := rpc.NewServer()
	if err := rpcServer.Register(me); err != nil {
		log.Fatal(err)
	}
	me.Mux.HandleFunc(rpc.DefaultRPCPath,
		func(w http.ResponseWriter, req *http.Request) {
			rpcServer.ServeHTTP(w, req)
		})

	addr := fmt.Sprintf(":%d", port)
	var err os.Error
	me.listener, err = net.Listen("tcp", addr)
	if err != nil {
		log.Fatal("net.Listen: ", err.String())
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
