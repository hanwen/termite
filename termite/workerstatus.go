package termite

import (
	"fmt"
	"http"
)

func (me *WorkerDaemon) httpHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "<html><head><title>Termite worker</head></title>")
	fmt.Fprintf(w, "<h1>Termite worker status</h1>")
	fmt.Fprintf(w, "<body><pre>")
	me.masterMapMutex.Lock()
	defer me.masterMapMutex.Unlock()

	for k, v := range me.masterMap {
		fmt.Fprintf(w, "\n******\nMirror: %s\n\n", k)
		v.httpHandler(w, r)
	}
	fmt.Fprintf(w, "</pre></body></html>")
}

func (me *Mirror) httpHandler(w http.ResponseWriter, r *http.Request) {
	me.fuseFileSystemsMutex.Lock()
	defer me.fuseFileSystemsMutex.Unlock()

	for _, v := range me.workingFileSystems {
		fmt.Fprintf(w, "FS:\n%s\n", v)
	}
	fmt.Fprintf(w, "%d unused filesystems.", len(me.fuseFileSystems))
}

func (me *WorkerDaemon) ServeHTTPStatus(port int) {
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		me.httpHandler(w, r)
	})
	http.ListenAndServe(fmt.Sprintf(":%d", port), nil)
}



