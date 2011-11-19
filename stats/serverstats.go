package stats

import (
	"fmt"
	"log"
	"net/http"
	"sync"
)

var _ = log.Println

type ServerStats struct {
	mutex         sync.Mutex
	phaseCounts   map[string]int
	*CpuStatSampler
	PhaseOrder    []string 
}

func NewServerStats() *ServerStats {
	return &ServerStats{
		phaseCounts: map[string]int{},
		CpuStatSampler:    NewCpuStatSampler(),
	}
}

func (me *ServerStats) Enter(phase string) {
	me.mutex.Lock()
	defer me.mutex.Unlock()
	me.phaseCounts[phase]++
}

func (me *ServerStats) Exit(phase string) {
	me.mutex.Lock()
	defer me.mutex.Unlock()
	me.phaseCounts[phase]--
}

func (me *ServerStats) WriteHttp(w http.ResponseWriter) {
	// TODO - share code with coordinator HTTP code.
	minuteStat := CpuStat{}
	fiveSecStat := CpuStat{}
	stats := me.CpuStats()
	if len(stats) > 0 {
		s := 0
		for i, c := range stats {
			minuteStat = minuteStat.Add(c)
			if i >= len(stats)-5 {
				s++
				fiveSecStat = fiveSecStat.Add(c)
			}
		}

		totalm := float64(minuteStat.SelfCpu+minuteStat.SelfSys) / 1.0e9
		fmt.Fprintf(w, "<p>CPU (last min): %d s self %d s sys, %.2f CPU",
			minuteStat.SelfCpu/1e9, minuteStat.SelfSys/1e9, totalm/float64(len(stats)))
		fmt.Fprintf(w, "<p>CPU (last %ds): %.2f self %.2f sys, %.1f CPU", s,
			float64(fiveSecStat.SelfCpu)*1e-9, float64(fiveSecStat.SelfSys)*1e-9,
			float64(fiveSecStat.SelfCpu+fiveSecStat.SelfSys)/float64(s*1e9))
	}
	me.mutex.Lock()
	defer me.mutex.Unlock()

	fmt.Fprintf(w, "<ul>")
	for _, k := range me.PhaseOrder {
		fmt.Fprintf(w, "<li>Jobs in phase %s: %d ", k, me.phaseCounts[k])
	}
	fmt.Fprintf(w, "</ul>")
}

func (me *ServerStats) PhaseCounts() (r []int) {
	me.mutex.Lock()
	defer me.mutex.Unlock()
	for _, n := range me.PhaseOrder {
		r = append(r, me.phaseCounts[n]) 
	}
	return r
}
