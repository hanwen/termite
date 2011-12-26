package termite
import (
	"fmt"
	"github.com/hanwen/termite/cba"
	"log"
	"net/http"
)

func (me *Master) sizeHistogram() (histo []int, total int) {
	for _, f := range me.attributes.Copy().Files {
		if !f.Deletion() && f.IsRegular() {
			e := IntToExponent(int(f.Size))
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
		fmt.Fprintf(w, "%d%s: %d (%d %%), ", 1<<uint(e), suffix, h, (100*cum)/total)
	}

	fmt.Fprintf(w, "<p>ContentCache memory hit rate: %.0f %%", 100.0*me.contentStore.MemoryHitRate())
	msgs := me.fileServer.stats.TimingMessages()
	msgs = append(msgs, me.contentStore.TimingMessages()...)
	fmt.Fprintf(w, "<ul>")
	for _, msg := range msgs {
		fmt.Fprintf(w, "<li>%s", msg)
	}
	fmt.Fprintf(w, "</ul>")

	me.mirrors.stats.WriteHttp(w)

	me.writeThroughput(w)
	
	fmt.Fprintf(w, "<p>Master parallelism (--jobs): %d. Reserved job slots: %d",
		me.mirrors.wantedMaxJobs, me.mirrors.maxJobs())
	fmt.Fprintf(w, "</body></html>")
}

func (me *Master) writeThroughput(w http.ResponseWriter) {
	throughput := me.contentStore.ThroughputStats()

	if len(throughput) > 0 {
		fmt.Fprintf(w, "<table>%s\n", throughput[0].TableHeader())
		total := &cba.ThroughputSample{}
		for i, t := range throughput {
			if i > len(throughput)-5 {
				fmt.Fprintf(w, "%s\n", t.TableRow())
			}
			total.AddSample(t)
		}
		fmt.Fprintf(w, "</table>")

		fmt.Fprintf(w, "Last %ds: %v", len(throughput), total)
	}
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
