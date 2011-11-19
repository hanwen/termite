package stats

import (
	"fmt"
	"log"
	"net/http"
	"sync"
)

var _ = log.Println

type ServerStats struct {
	mutex       sync.Mutex
	phaseCounts map[string]int
	*CpuStatSampler
	PhaseOrder []string
}

func NewServerStats() *ServerStats {
	return &ServerStats{
		phaseCounts:    map[string]int{},
		CpuStatSampler: NewCpuStatSampler(),
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

func CpuStatsWriteHttp(w http.ResponseWriter, stats []CpuStat) {
	statm := CpuStat{}
	stat5 := CpuStat{}
	if len(stats) > 0 {
		count5 := int64(0)
		fmt.Fprintf(w, "<p><table><tr><th>self cpu (ms)</th><th>self sys (ms)</th>"+
			"<th>child cpu (ms)</th><th>child sys (ms)</th><th>total</th></tr>")
		for i, v := range stats {
			statm = statm.Add(v)
			if i >= len(stats)-5 {
				count5++
				stat5 = stat5.Add(v)
				fmt.Fprintf(w, "<tr><td>%d</td><td>%d</td><td>%d</td><td>%d</td><td>%d</td></tr>",
					v.SelfCpu/1e6, v.SelfSys/1e6, v.ChildCpu/1e6, v.ChildSys/1e6,
					(v.SelfCpu+v.SelfSys+v.ChildCpu+v.ChildSys)/1e6)
			}
		}
		fmt.Fprintf(w, "</table>")

		totalm := float64(statm.SelfCpu+statm.SelfSys) / 1.0e9
		fmt.Fprintf(w, "<p>CPU (last min): %d s self %d s sys, %.2f CPU",
			statm.SelfCpu/1e9, statm.SelfSys/1e9, totalm/float64(len(stats)))
		fmt.Fprintf(w, "<p>CPU (last %ds): %.2f self %.2f sys, %.1f CPU", count5,
			float64(stat5.SelfCpu)*1e-9, float64(stat5.SelfSys)*1e-9,
			float64(stat5.SelfCpu+stat5.SelfSys)/float64(count5*1e9))
	}
}

func (me *ServerStats) WriteHttp(w http.ResponseWriter) {
	// TODO - share code with coordinator HTTP code.
	CpuStatsWriteHttp(w, me.CpuStats())
	counts := me.PhaseCounts()
	fmt.Fprintf(w, "<ul>")
	for i, c := range counts {
		fmt.Fprintf(w, "<li>Jobs in phase %s: %d ", me.PhaseOrder[i], c)
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
