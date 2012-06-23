package termite

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/http/pprof"
	"os"

	"github.com/hanwen/termite/stats"
)

func serveLog(worker *Worker, w http.ResponseWriter, req *http.Request) {
	sz := int64(500 * 1024)
	sizeStr, ok := req.URL.Query()["size"]
	if ok {
		fmt.Scanf(sizeStr[0], "%d", &sz)
	}

	logReq := LogRequest{Whence: os.SEEK_END, Off: -sz, Size: sz}
	logRep := LogResponse{}
	err := worker.Log(&logReq, &logRep)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "<html><head><title>Termite worker error</title></head>")
		fmt.Fprintf(w, "<body>Error: %s</body></html>", err.Error())
		return
	}

	w.Header().Set("Content-Type", "text/plain")
	w.Write(logRep.Data)
}

func serveStatus(worker *Worker, w http.ResponseWriter, r *http.Request) {
	statusReq := WorkerStatusRequest{}
	status := WorkerStatusResponse{}

	err := worker.Status(&statusReq, &status)
	if err != nil {
		// TODO - escaping.
		fmt.Fprintf(w, "<p>Status error: %v", err)
	}

	addr := fmt.Sprintf("%s:%d", cname, worker.listener.Addr().(*net.TCPAddr).Port)
	fmt.Fprintf(w, "<p>Worker %s (<a href=\"http://%s:%d\">status</a>)<p>Version %s<p>Jobs %d\n",
		addr, cname, worker.httpStatusPort, status.Version, status.MaxJobCount)
	fmt.Fprintf(w, "<p><a href=\"/log?host=%s\">Worker log %s</a>\n", addr, addr)

	if !status.Accepting {
		fmt.Fprintf(w, "<b>shutting down</b>")
	}
	stats.CpuStatsWriteHttp(w, status.CpuStats, status.DiskStats)

	fmt.Fprintf(w, "<p>Total CPU: %s", status.TotalCpu.Percent())
	fmt.Fprintf(w, "<p>Content cache hit rate: %.0f %%, Age %d",
		100.0*status.ContentCacheHitRate,
		status.ContentCacheHitAge)

	m := status.MemStat
	fmt.Fprintf(w, "<p>HeapIdle: %v, HeapInUse: %v",
		m.HeapIdle, m.HeapInuse)

	stats.CountStatsWriteHttp(w, status.PhaseNames, status.PhaseCounts)

	for _, mirrorStatus := range status.MirrorStatus {
		mirrorStatusHtml(w, mirrorStatus)
	}
}

func mirrorStatusHtml(w http.ResponseWriter, s MirrorStatusResponse) {
	fmt.Fprintf(w, "<h2>Mirror %s</h2>", s.Root)
	for _, s := range s.RpcTimings {
		fmt.Fprintf(w, "<li>%s", s)
	}

	running := 0
	for _, fs := range s.Fses {
		running += len(fs.Tasks)
	}

	fmt.Fprintf(w, "<p>%d maximum jobs, %d running, %d waiting tasks, %d unused filesystems.\n",
		s.Granted, running, s.WaitingTasks, s.IdleFses)
	if !s.Accepting {
		fmt.Fprintf(w, "<p><b>shutting down</b>\n")
	}

	fmt.Fprintf(w, "<ul>\n")
	for _, v := range s.Fses {
		fmt.Fprintf(w, "<li>id %s: %s<ul>\n", v.Id, v.Mem)
		for _, t := range v.Tasks {
			fmt.Fprintf(w, "<li>%s\n", t)
		}
		fmt.Fprintf(w, "</ul>\n")
	}
	fmt.Fprintf(w, "</ul>\n")
}

func (w *Worker) serveStatus() {
	l, err := net.Listen("tcp", ":0")
	if err != nil {
		log.Println("status serve:", err)
		return
	}

	w.httpStatusPort = l.Addr().(*net.TCPAddr).Port
	log.Printf("Serving status on port %d", w.httpStatusPort)
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(wr http.ResponseWriter, r *http.Request) {
		serveStatus(w, wr, r)
	})
	mux.HandleFunc("/log", func(wr http.ResponseWriter, r *http.Request) {
		serveLog(w, wr, r)
	})

	mux.Handle("/debug/pprof/", http.HandlerFunc(pprof.Index))
	mux.Handle("/debug/pprof/cmdline", http.HandlerFunc(pprof.Cmdline))
	mux.Handle("/debug/pprof/profile", http.HandlerFunc(pprof.Profile))
	mux.Handle("/debug/pprof/symbol", http.HandlerFunc(pprof.Symbol))

	err = http.Serve(l, mux)
	if err != nil {
		log.Println("status serve:", err)
		return
	}
}
