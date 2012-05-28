package termite

import (
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"sort"
	"syscall"
	"sync"
	"time"
)

var _ = log.Println

type Registration struct {
	Address string
	Name    string
	Version string
	HttpStatusPort int
}

type Registered struct {
	Registrations []Registration
}

type WorkerRegistration struct {
	Registration
	LastReported time.Time
}

// Coordinator is the registration service for termite.  Workers
// register here.  A master looking for workers contacts the
// Coordinator to fetch a list of available workers.  In addition, it
// has a HTTP interface to inspect each worker.
type Coordinator struct {
	Mux *http.ServeMux

	options *CoordinatorOptions

	listener net.Listener
	mutex    sync.Mutex
	workers  map[string]*WorkerRegistration
}

type CoordinatorOptions struct {
	// Secret is the password for coordinator, workers and master
	// to authenticate.
	Secret []byte

	// Password should be passed in the kill/restart URLs to make
	// sure web scrapers don't randomly shutdown workers.
	WebPassword string
}

func NewCoordinator(opts *CoordinatorOptions) *Coordinator {
	o := *opts
	return &Coordinator{
		options: &o,
		workers: make(map[string]*WorkerRegistration),
		Mux:     http.NewServeMux(),
	}
}

func (me *Coordinator) Register(req *Registration, rep *int) error {
	conn, err := DialTypedConnection(req.Address, RPC_CHANNEL, me.options.Secret)
	if conn != nil {
		conn.Close()
	}
	if err != nil {
		return errors.New(fmt.Sprintf(
			"error contacting address: %v", err))
	}

	me.mutex.Lock()
	defer me.mutex.Unlock()

	w := &WorkerRegistration{Registration: *req}
	w.LastReported = time.Now()
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
	now := time.Now()

	addrs := me.workerAddresses()

	var toDelete []string
	for _, a := range addrs {
		conn, err := DialTypedConnection(a, RPC_CHANNEL, me.options.Secret)
		if err != nil {
			toDelete = append(toDelete, a)
		} else {
			conn.Close()
		}
	}

	me.mutex.Lock()
	for _, a := range toDelete {
		w := me.workers[a]
		if w != nil && now.After(w.LastReported) {
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
		addr := worker.Address
		fmt.Fprintf(w, "<li><a href=\"worker?host=%s\">address <tt>%s</tt>, host <tt>%s</tt></a>"+
			" (<a href=\"/workerkill?host=%s\">Kill</a>, \n" +
			"<a href=\"/restart?host=%s\">Restart</a>)\n", 
			addr, addr, worker.Name, addr, addr)
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

func (me *Coordinator) killAll(restart bool) error {
	addrs := me.workerAddresses()
	done := make(chan error, len(addrs))
	for _, w := range addrs {
		go func(w string) {
			err := me.killWorker(w, restart)
			done <- err
		}(w)
	}

	errs := []error{}
	for _ = range addrs {
		e := <-done
		if e != nil {
			errs = append(errs, e)
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("%v", errs)
	}
	return nil
}

func (me *Coordinator) killWorker(addr string, restart bool) error {
	conn, err := DialTypedConnection(addr, RPC_CHANNEL, me.options.Secret)
	if err == nil {
		killReq := ShutdownRequest{Restart: restart}
		rep := ShutdownResponse{}
		cl := rpc.NewClient(conn)
		defer cl.Close()
		err = cl.Call("Worker.Shutdown", &killReq, &rep)
	}
	return err
}

func (me *Coordinator) killAllHandler(w http.ResponseWriter, req *http.Request) {
	me.log(req)
	if !me.checkPassword(w, req) {
		return
	}

	w.Header().Set("Content-Type", "text/html")
	fmt.Fprintf(w, "<p>%s in progress", req.URL.Path)
	err := me.killAll(req.URL.Path == "/restartall")
	if err != nil {
		fmt.Fprintf(w, "error: %v", err)
	}
	// Should have a redirect.
	fmt.Fprintf(w, "<p><a href=\"/\">back to index</a>")
	go me.checkReachable()
}

func (me *Coordinator) checkPassword(w http.ResponseWriter, req *http.Request) bool {
	if me.options.WebPassword == "" {
		return true
	}
	q := req.URL.Query()
	pw := q["pw"]
	if len(pw) == 0 || pw[0] != me.options.WebPassword {
		fmt.Fprintf(w, "<html><body>unauthorized &amp;pw=PASSWORD missing or incorrect.</body></html>")
		return false
	}
	return true
}

func (me *Coordinator) killHandler(w http.ResponseWriter, req *http.Request) {
	me.log(req)
	if !me.checkPassword(w, req) {
		return
	}

	addr, err := me.getHost(req)
	var conn net.Conn
	if err == nil {
		conn, err = DialTypedConnection(addr, RPC_CHANNEL, me.options.Secret)
	}
	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w, "<html><head><title>Termite worker error</title></head>")
		fmt.Fprintf(w, "<body>Error: %s</body></html>", err.Error())
		return
	}

	defer conn.Close()

	w.Header().Set("Content-Type", "text/html")
	restart := req.URL.Path == "/restart"
	fmt.Fprintf(w, "<html><head><title>Termite worker status</title></head>")
	fmt.Fprintf(w, "<body><h1>Status %s</h1>", addr)
	defer fmt.Fprintf(w, "</body></html>")

	killReq := ShutdownRequest{
		Restart: restart,
		Kill:    !restart,
	}
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
	conn, err := DialTypedConnection(addr, RPC_CHANNEL, me.options.Secret)
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

func (me *Coordinator) getWorker(addr string) *WorkerRegistration {
	me.mutex.Lock()
	defer me.mutex.Unlock()
	return me.workers[addr]
}

func (me *Coordinator) getHost(req *http.Request) (string, error) {
	q := req.URL.Query()
	vs, ok := q["host"]
	if !ok || len(vs) == 0 {
		return "", fmt.Errorf("query param 'host' missing")
	}
	addr := string(vs[0])
	if me.getWorker(addr) == nil {
		return "", fmt.Errorf("worker %q unknown", addr)
	}

	return addr, nil
}

func (me *Coordinator) workerHandler(w http.ResponseWriter, req *http.Request) {
	addr, err := me.getHost(req)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "<html><head><title>Termite worker status</title></head>")
		fmt.Fprintf(w, "<body>Error: %s</body></html>", err.Error())
		return
	}
	workerData := me.getWorker(addr)
	host, _, _ := net.SplitHostPort(addr)
	resp, err := http.Get(fmt.Sprintf("http://%s:%d/%s?%s", host, workerData.HttpStatusPort,
	req.URL.Path, req.URL.RawQuery))

	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "<html><head><title>Termite worker status</title></head>")
		fmt.Fprintf(w, "<body>Error: %s</body></html>", err.Error())
		return
	}

	io.Copy(w, resp.Body)
	resp.Body.Close()
}

func (me *Coordinator) Shutdown() {
	log.Println("Coordinator shutdown.")
	me.listener.Close()
}

func (me *Coordinator) log(req *http.Request) {
	log.Printf("from %v: %v", req.RemoteAddr, req.URL)
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
	if e, ok := err.(*net.OpError); ok && e.Err == syscall.EINVAL {
		return
	}

	if err != nil && err != syscall.EINVAL {
		log.Println("httpServer.Serve:", err)
	}
}
