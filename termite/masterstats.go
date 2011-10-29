package termite

import (
	"fmt"
	"http"
	"log"
	"sync"
)

var _ = log.Println

type masterStats struct {
	counterMutex sync.Mutex
	phaseCounts   map[string]int
	cpuStats      *cpuStatSampler
}

func newMasterStats() *masterStats {
	return &masterStats{
		phaseCounts:      map[string]int{},
		cpuStats:         newCpuStatSampler(),
	}
}

func (me *masterStats) Enter(phase string) {
	me.counterMutex.Lock()
	defer me.counterMutex.Unlock()
	me.phaseCounts[phase]++
}

func (me *masterStats) Exit(phase string) {
	me.counterMutex.Lock()
	defer me.counterMutex.Unlock()
	me.phaseCounts[phase]--
}

func (me *masterStats) writeHttp(w http.ResponseWriter) {
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
	me.counterMutex.Lock()
	defer me.counterMutex.Unlock()

	for _, k := range []string{"run", "send", "remote", "filewait"} {
		fmt.Fprintf(w, "<p>Jobs in %s status: %d ", k, me.phaseCounts[k])
	}
}
