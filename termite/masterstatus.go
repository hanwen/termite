package termite

import (
	"fmt"
	"http"
	"log"
	"time"
)

func (me *Master) statusHandler(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "text/html")

	fmt.Fprintf(w, "<html><head><title>Master status</title></head>")
	fmt.Fprintf(w, "<body><h1>Master for %s</h1>", me.writableRoot)
	defer fmt.Fprintf(w, "</body></html>")

	me.jobStats.Add(time.Seconds(), 0)
	fmt.Fprintf(w, "<p>Jobs (sec/min/10min): %v", me.jobStats.Read())
}

func (me *Master) ServeHTTP(port int) {
	http.HandleFunc("/",
		func(w http.ResponseWriter, req *http.Request) {
			me.statusHandler(w, req)
		})

	addr := fmt.Sprintf(":%d", port)
	log.Println("HTTP status on", addr)
	err := http.ListenAndServe(addr, nil)
	if err != nil {
		log.Println("http serve error:", err)
	}
}
