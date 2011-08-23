package termite

import (
	"fmt"
	"http"
	"log"
	"os"
	"rpc"
	"sync"
	"time"
)

var _ = log.Println

type Registration struct {
	Address           string
	Name              string
	Version           string
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

	mutex   sync.Mutex
	workers map[string]*WorkerRegistration
	secret  []byte
}

func NewCoordinator(secret []byte) *Coordinator {
	return &Coordinator{
		workers: make(map[string]*WorkerRegistration),
		secret:  secret,
		Mux: http.NewServeMux(),
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

	me.mutex.Lock()
	var addrs []string
	for k, _ := range me.workers {
		addrs = append(addrs, k)
	}
	me.mutex.Unlock()

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
	defer fmt.Fprintf(w, "</ul></body></html>")

	for _, worker := range me.workers {
		fmt.Fprintf(w, "<li><a href=\"worker?host=%s\">address <tt>%s</tt>, host <tt>%s</tt></a>",
			worker.Address, worker.Address, worker.Name)
	}
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
	me.mutex.Lock()
	defer me.mutex.Unlock()

	_, ok = me.workers[addr]

	fmt.Fprintf(w, "<html><head><title>Termite worker status</title></head>")
	fmt.Fprintf(w, "<body><h1>Status %s</h1>", addr)
	defer fmt.Fprintf(w, "</body></html>")

	if !ok {
		fmt.Fprintf(w, "<p><tt>worker %q unknown<tt>", addr)
		return
	}

	conn, err := DialTypedConnection(addr, RPC_CHANNEL, me.secret)
	if err != nil {
		fmt.Fprintf(w, "<p><tt>error dialing: %v<tt>", err)
		return
	}

	killReq := 1
	rep := 1
	err = rpc.NewClient(conn).Call("WorkerDaemon.Shutdown", &killReq, &rep)
	if err != nil {
		fmt.Fprintf(w, "<p><tt>RPC error: %v<tt>", err)
		return
	}
	
	fmt.Fprintf(w, "<p>Shutdown of %s in progress", addr)
	conn.Close()
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
	me.mutex.Lock()
	defer me.mutex.Unlock()

	_, ok = me.workers[addr]

	fmt.Fprintf(w, "<html><head><title>Termite worker status</title></head>")
	fmt.Fprintf(w, "<body><h1>Status %s</h1>", addr)
	defer fmt.Fprintf(w, "</body></html>")

	if !ok {
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
	if err != nil {
		fmt.Fprintf(w, "<p><tt>RPC error: %v<tt>\n", err)
		return
	}

	fmt.Fprintf(w, "<p>Worker %s<p>Version %s<p>Jobs %d\n", addr, status.Version, status.MaxJobCount)
	for _, mirrorStatus := range status.MirrorStatus {
		me.mirrorStatusHtml(w, mirrorStatus)
	}
	fmt.Fprintf(w, "<p><a href=\"/workerkill?host=%s\">Kill worker %s</a>\n", addr, addr)
	conn.Close()
}

func (me *Coordinator) mirrorStatusHtml(w http.ResponseWriter, s MirrorStatusResponse) {
	fmt.Fprintf(w, "<h2>Mirror %s</h2>", s.Root)
	fmt.Fprintf(w, "<p>%d maximum jobs, %d running, %d waiting tasks, %d unused filesystems.\n",
		s.Granted, len(s.Running), s.WaitingTasks, s.IdleFses)
	if s.ShuttingDown {
		fmt.Fprintf(w, "<p><b>shutting down</b>\n")
	}

	for _, v := range s.Running {
		fmt.Fprintf(w, "<p>FS:\n%s\n", v)
	}
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
	me.Mux.HandleFunc("/workerkill",
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
	log.Println("Coordinator listening on", addr)

	httpServer := http.Server{
		Addr: addr,
		Handler: me.Mux,
	}
	err := httpServer.ListenAndServe()
	if err != nil {
		log.Fatal("ListenAndServe: ", err.String())
	}
}
