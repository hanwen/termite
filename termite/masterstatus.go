package termite

import (
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"

	"github.com/hanwen/termite/cba"
)

func (m *Master) sizeHistogram() (histo []int, total int) {
	for _, f := range m.attributes.Copy().Files {
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

func (m *Master) statusHandler(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "text/html")

	fmt.Fprintf(w, "<html><head><title>Master status</title></head>")
	fmt.Fprintf(w, "<body><h1>Master for %s</h1>", m.options.WritableRoot)

	fmt.Fprintf(w, "<p>%s", Version())
	histo, total := m.sizeHistogram()
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

	msgs := m.fileServer.TimingMessages()
	msgs = append(msgs, m.contentStore.TimingMessages()...)
	fmt.Fprintf(w, "<ul>")
	for _, msg := range msgs {
		fmt.Fprintf(w, "<li>%s", msg)
	}
	fmt.Fprintf(w, "</ul>")

	m.mirrors.stats.WriteHttp(w)

	m.writeThroughput(w)

	fmt.Fprintf(w, "<p>Master parallelism (--jobs): %d. Reserved job slots: %d",
		m.mirrors.wantedMaxJobs, m.mirrors.maxJobs())
	fmt.Fprintf(w, "</body></html>")
}

func (m *Master) writeThroughput(w http.ResponseWriter) {
	throughput := m.contentStore.ThroughputStats()

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

func (m *Master) ServeHTTP(port int) {
	http.HandleFunc("/",
		func(w http.ResponseWriter, req *http.Request) {
			m.statusHandler(w, req)
		})
	addr := fmt.Sprintf(":%d", port)
	log.Println("HTTP status on", addr)
	err := http.ListenAndServe(addr, nil)
	if err != nil {
		log.Println("http serve error:", err)
	}
}
