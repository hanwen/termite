package termite

import (
	"fmt"
	"http"
)

func (me *WorkerDaemon) httpHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "<html><head><title>Termite worker</head></title>")
	fmt.Fprintf(w, "<h1>Termite worker status</h1>")
	fmt.Fprintf(w, "<body>")
	me.mirrorMapMutex.Lock()
	defer me.mirrorMapMutex.Unlock()

	for k, v := range me.mirrorMap {
		fmt.Fprintf(w, "<h2>Mirror</h2><p><tt>%s</tt>\n", k)
		v.httpHandler(w, r)
	}
	fmt.Fprintf(w, "</body></html>")
}

func (me *Mirror) httpHandler(w http.ResponseWriter, r *http.Request) {
	me.fuseFileSystemsMutex.Lock()
	defer me.fuseFileSystemsMutex.Unlock()

	for _, v := range me.workingFileSystems {
		fmt.Fprintf(w, "<p>FS:\n%s\n", v)
	}
	fmt.Fprintf(w, "<p>%d unused filesystems.", len(me.fuseFileSystems))
}

func (me *WorkerDaemon) ServeHTTPStatus(port int) {
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		me.httpHandler(w, r)
	})
	http.ListenAndServe(fmt.Sprintf(":%d", port), nil)
}
