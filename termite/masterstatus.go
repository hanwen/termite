package termite

import (
	"fmt"
	"github.com/hanwen/go-fuse/fuse"
	"http"
	"log"
)

func (me *Master) sizeHistogram() (histo []int, total int) {
	for _, f := range me.attr.Copy().Files {
		if f.FileInfo != nil && f.IsRegular() {
			e := fuse.IntToExponent(int(f.Size))
			for len(histo) <= int(e) {
				histo = append(histo, 0)
			}
			histo[e]++
			total++
		}
	}
	return histo, total
}

func (me *Master) statusHandler(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "text/html")

	fmt.Fprintf(w, "<html><head><title>Master status</title></head>")
	fmt.Fprintf(w, "<body><h1>Master for %s</h1>", me.options.WritableRoot)

	fmt.Fprintf(w, "<p>%s", Version())
	histo, total := me.sizeHistogram()
	fmt.Fprintf(w, "<p>Filesizes of %d files: ", total)

	cum := 0
	for e, h := range histo {
		if h == 0 {
			continue
		}
		suffix := ""
		switch {
		case e >= 20:
			suffix = "M"
			e -= 20
		case e >= 10:
			suffix = "K"
			e -= 10
		}
		cum += h
		fmt.Fprintf(w, "%d%s: %d (%d %%), ", 1 << uint(e), suffix, h, (100*cum)/total)
	}

	fmt.Fprintf(w, "<p>ContentCache memory hit rate: %.0f %%", 100.0*me.cache.MemoryHitRate())

	me.mirrors.stats.writeHttp(w)

	fmt.Fprintf(w, "<p>Master parallelism (--jobs): %d. Reserved job slots: %d",
		me.mirrors.wantedMaxJobs, me.mirrors.maxJobs())
	fmt.Fprintf(w, "</body></html>")
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
