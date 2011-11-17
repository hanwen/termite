package termite

import (
	"fmt"
	"log"
	"net/http"
	"sync"
)

var _ = log.Println

type serverStats struct {
	mutex         sync.Mutex
	phaseCounts   map[string]int
	cpuStats      *cpuStatSampler
	phaseOrder    []string 
}

func newServerStats() *serverStats {
	return &serverStats{
		phaseCounts: map[string]int{},
		cpuStats:    newCpuStatSampler(),
	}
}

func (me *serverStats) Enter(phase string) {
	me.mutex.Lock()
	defer me.mutex.Unlock()
	me.phaseCounts[phase]++
}

func (me *serverStats) Exit(phase string) {
	me.mutex.Lock()
	defer me.mutex.Unlock()
	me.phaseCounts[phase]--
}

func (me *serverStats) writeHttp(w http.ResponseWriter) {
	// TODO - share code with coordinator HTTP code.
	minuteStat := CpuStat{}
	fiveSecStat := CpuStat{}
	stats := me.cpuStats.CpuStats()
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
	for _, k := range me.phaseOrder {
		fmt.Fprintf(w, "<li>Jobs in phase %s: %d ", k, me.phaseCounts[k])
	}
	fmt.Fprintf(w, "</ul>")
}

func (me *serverStats) FillWorkerStatus(rep *WorkerStatusResponse) {
	me.mutex.Lock()
	defer me.mutex.Unlock()
	rep.CpuStats = me.cpuStats.CpuStats()
	for _, n := range me.phaseOrder {
		rep.PhaseNames = append(rep.PhaseNames, n) 
		rep.PhaseCounts = append(rep.PhaseCounts, me.phaseCounts[n]) 
	}
}
