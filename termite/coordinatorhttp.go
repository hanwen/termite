package termite

import (
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"sort"
	"syscall"
	"time"
)

func (c *Coordinator) getHost(req *http.Request) (string, error) {
	q := req.URL.Query()
	vs, ok := q["host"]
	if !ok || len(vs) == 0 {
		return "", fmt.Errorf("query param 'host' missing")
	}
	addr := string(vs[0])
	if c.getWorker(addr) == nil {
		return "", fmt.Errorf("worker %q unknown", addr)
	}

	return addr, nil
}

func (c *Coordinator) workerHandler(w http.ResponseWriter, req *http.Request) {
	addr, err := c.getHost(req)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "<html><head><title>Termite worker status</title></head>")
		fmt.Fprintf(w, "<body>Error: %s</body></html>", err.Error())
		return
	}
	workerData := c.getWorker(addr)
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

func (c *Coordinator) ServeHTTP(port int) {
	c.Mux.HandleFunc("/",
		func(w http.ResponseWriter, req *http.Request) {
			c.rootHandler(w, req)
		})
	c.Mux.HandleFunc("/worker",
		func(w http.ResponseWriter, req *http.Request) {
			c.workerHandler(w, req)
		})
	c.Mux.HandleFunc("/log",
		func(w http.ResponseWriter, req *http.Request) {
			c.workerHandler(w, req)
		})
	c.Mux.HandleFunc("/shutdown",
		func(w http.ResponseWriter, req *http.Request) {
			c.shutdownSelf(w, req)
		})
	c.Mux.HandleFunc("/workerkill",
		func(w http.ResponseWriter, req *http.Request) {
			c.killHandler(w, req)
		})
	c.Mux.HandleFunc("/killall",
		func(w http.ResponseWriter, req *http.Request) {
			c.killAllHandler(w, req)
		})
	c.Mux.HandleFunc("/restartall",
		func(w http.ResponseWriter, req *http.Request) {
			c.killAllHandler(w, req)
		})
	c.Mux.HandleFunc("/restart",
		func(w http.ResponseWriter, req *http.Request) {
			c.killHandler(w, req)
		})

	rpcServer := rpc.NewServer()
	if err := rpcServer.RegisterName("Coordinator", (*CoordinatorService)(c)); err != nil {
		log.Fatal("rpcServer.Register:", err)
	}
	c.Mux.HandleFunc(rpc.DefaultRPCPath,
		func(w http.ResponseWriter, req *http.Request) {
			rpcServer.ServeHTTP(w, req)
		})

	addr := fmt.Sprintf(":%d", port)
	var err error
	c.listener, err = net.Listen("tcp", addr)
	if err != nil {
		log.Fatal("net.Listen: ", err.Error())
	}
	log.Println("Coordinator listening on", addr)

	httpServer := http.Server{
		Addr:    addr,
		Handler: c.Mux,
	}
	err = httpServer.Serve(c.listener)
	if e, ok := err.(*net.OpError); ok && e.Err == syscall.EINVAL {
		return
	}

	if err != nil && err != syscall.EINVAL {
		log.Println("httpServer.Serve:", err)
	}
}

func (c *Coordinator) killAllHandler(w http.ResponseWriter, req *http.Request) {
	c.log(req)
	if !c.checkPassword(w, req) {
		return
	}

	w.Header().Set("Content-Type", "text/html")
	fmt.Fprintf(w, "<p>%s in progress", req.URL.Path)
	err := c.killAll(req.URL.Path == "/restartall")
	if err != nil {
		fmt.Fprintf(w, "error: %v", err)
	}
	// Should have a redirect.
	fmt.Fprintf(w, "<p><a href=\"/\">back to index</a>")
	go c.checkReachable()
}

func (c *Coordinator) checkPassword(w http.ResponseWriter, req *http.Request) bool {
	if c.options.WebPassword == "" {
		return true
	}
	q := req.URL.Query()
	pw := q["pw"]
	if len(pw) == 0 || pw[0] != c.options.WebPassword {
		fmt.Fprintf(w, "<html><body>unauthorized &amp;pw=PASSWORD missing or incorrect.</body></html>")
		return false
	}
	return true
}

func (c *Coordinator) killHandler(w http.ResponseWriter, req *http.Request) {
	c.log(req)
	if !c.checkPassword(w, req) {
		return
	}

	addr, err := c.getHost(req)
	var conn io.ReadWriteCloser
	if err == nil {
		conn, err = c.dialWorker(addr)
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
	fmt.Fprintf(w, "<p>%s of %s in progress", action, addr)
	// Should have a redirect.
	fmt.Fprintf(w, "<p><a href=\"/\">back to index</a>")
	go c.checkReachable()
}

func (c *Coordinator) rootHandler(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "text/html")
	c.mutex.Lock()
	defer c.mutex.Unlock()

	fmt.Fprintf(w, "<html><head><title>Termite coordinator</title></head>")
	fmt.Fprintf(w, "<body><h1>Termite coordinator</h1><ul>")
	fmt.Fprintf(w, "<p>version %s", Version())
	defer fmt.Fprintf(w, "</body></html>")

	keys := []string{}
	for k := range c.workers {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, k := range keys {
		worker := c.workers[k]
		addr := worker.Address
		fmt.Fprintf(w, "<li><a href=\"worker?host=%s\">address <tt>%s</tt>, host <tt>%s</tt></a>"+
			" (<a href=\"/workerkill?host=%s\">Kill</a>, \n"+
			"<a href=\"/restart?host=%s\">Restart</a>)\n",
			addr, addr, worker.Name, addr, addr)
	}
	fmt.Fprintf(w, "</ul>")

	fmt.Fprintf(w, "<hr><p><a href=\"killall\">kill all workers,</a>"+
		"<a href=\"restartall\">restart all workers</a>")
}

func (c *Coordinator) shutdownSelf(w http.ResponseWriter, req *http.Request) {
	fmt.Fprintf(w, "<html><head><title>Termite coordinator</title></head>")
	fmt.Fprintf(w, "<body><h1>Shutdown in progress</h1><ul>")

	// Async, so we can still return the reply here.
	time.AfterFunc(100e6, func() { c.Shutdown() })
}

func (c *Coordinator) killAll(restart bool) error {
	addrs := c.workerAddresses()
	done := make(chan error, len(addrs))
	for _, w := range addrs {
		go func(w string) {
			err := c.killWorker(w, restart)
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
