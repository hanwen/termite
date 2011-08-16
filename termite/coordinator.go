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
	Address           string
	Name              string
	HttpStatusAddress string
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

type Coordinator struct {
	mutex   sync.Mutex
	workers map[string]*WorkerRegistration
	secret  []byte
}

func NewCoordinator(secret []byte) *Coordinator {
	return &Coordinator{
		workers: make(map[string]*WorkerRegistration),
		secret:  secret,
	}
}

func (me *Coordinator) Register(req *Registration, rep *int) os.Error {
	if !reachable(req.Address) {
		return os.NewError("error contacting address")
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

// TODO - use secret.
func reachable(addr string) bool {
	conn, _ := net.Dial("tcp", addr)
	ok := conn != nil
	if ok {
		conn.Close()
	}
	return ok
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
		if !reachable(a) {
			toDelete = append(toDelete, a)
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
	fmt.Fprintf(w, "<body><h1>Termite coordinator</h1>")
	defer fmt.Fprintf(w, "</body></html>")

	for _, worker := range me.workers {
		fmt.Fprintf(w, "<a href=\"worker?host=%s\">address <tt>%s</tt>, host <tt>%s</tt></a>",
			worker.Address, worker.Address, worker.Name)
	}
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
	}

	statusReq := WorkerStatusRequest{}
	status := WorkerStatusResponse{}

	client := rpc.NewClient(conn)
	err = client.Call("WorkerDaemon.Status", &statusReq, &status)
	if err != nil {
		fmt.Fprintf(w, "<p><tt>RPC error: %v<tt>", err)
		return
	}

	fmt.Fprintf(w, "<p>Worker %s<p>Version %s", addr, status.Version)
	for _, mirrorStatus := range status.MirrorStatus {
		me.mirrorStatusHtml(w, mirrorStatus)
	}
}

func (me *Coordinator) mirrorStatusHtml(w http.ResponseWriter, s MirrorStatusResponse) {
	fmt.Fprintf(w, "<h2>Mirror %s</h2>", s.Root)
	fmt.Fprintf(w, "<p>%d maximum jobs, %d running, %d waiting tasks, %d unused filesystems.",
		s.Granted, len(s.Running), s.WaitingTasks, s.IdleFses)
	if s.ShuttingDown {
		fmt.Fprintf(w, "<p><b>shutting down</b>")
	}

	for _, v := range s.Running {
		fmt.Fprintf(w, "<p>FS:\n%s\n", v)
	}
}

func (me *Coordinator) HandleHTTP() {
	http.HandleFunc("/",
		func(w http.ResponseWriter, req *http.Request) {
			me.rootHandler(w, req)
		})
	http.HandleFunc("/worker",
		func(w http.ResponseWriter, req *http.Request) {
			me.workerHandler(w, req)
		})
}
