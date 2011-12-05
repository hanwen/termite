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
	if len(stats) == 0 {
		return
	}

	statm := CpuStat{}
	stat5 := CpuStat{}
	count5 := int64(0)
	for i, v := range stats {
		statm = statm.Add(v)
		if i >= len(stats)-5 {
			count5++
			stat5 = stat5.Add(v)
		}
	}

	printChild := statm.ChildCpu+statm.ChildSys > 0
	chHeader := ""
	if printChild {
		chHeader = "<th>child cpu (ms)</th><th>child sys (ms)</th>"
	}
	fmt.Fprintf(w, "<p><table><tr><th>self cpu (ms)</th><th>self sys (ms)</th>%s<th>total</th></tr>",
		chHeader)
	for i, v := range stats {
		if i < len(stats)-5 {
			continue
		}

		chRow := ""
		if printChild {
			chRow = fmt.Sprintf("<td>%d</td><td>%d</td>", v.ChildCpu/1e6, v.ChildSys/1e6)
		}

		fmt.Fprintf(w, "<tr><td>%d</td><td>%d</td>%s<td>%d</td></tr>",
			v.SelfCpu/1e6, v.SelfSys/1e6, chRow,
			(v.Total())/1e6)
	}
	fmt.Fprintf(w, "</table>")

	fmt.Fprintf(w, "<p>CPU (last min): %d s self %d s sys", statm.SelfCpu/1e9, statm.SelfSys/1e9)
	if printChild {
		fmt.Fprintf(w, " %d s child %d s sys", statm.ChildCpu/1e9, statm.ChildSys/1e9)
	}

	fmt.Fprintf(w, " %.2f CPU", float64(statm.Total())/1e9/float64(len(stats)))
	fmt.Fprintf(w, "<p>CPU (last %ds): %.2f self %.2f sys",
		count5, float64(stat5.SelfCpu)*1e-9, float64(stat5.SelfSys)*1e-9)
	if printChild {
		fmt.Fprintf(w, "%.2f s child %.2f s sys", float64(stat5.ChildCpu)*1e-9, float64(stat5.ChildSys)*1e-9)
	}
	fmt.Fprintf(w, " %.2f CPU", float64(stat5.Total())/1e9/float64(count5))
}

func CountStatsWriteHttp(w http.ResponseWriter, names []string, counts []int) {
	fmt.Fprintf(w, "<ul>")
	for i, c := range counts {
		fmt.Fprintf(w, "<li>Jobs in phase %s: %d ", names[i], c)
	}
	fmt.Fprintf(w, "</ul>")
}

func (me *ServerStats) WriteHttp(w http.ResponseWriter) {
	CpuStatsWriteHttp(w, me.CpuStats())
	CountStatsWriteHttp(w, me.PhaseOrder, me.PhaseCounts())
}

func (me *ServerStats) PhaseCounts() (r []int) {
	me.mutex.Lock()
	defer me.mutex.Unlock()
	for _, n := range me.PhaseOrder {
		r = append(r, me.phaseCounts[n])
	}
	return r
}
