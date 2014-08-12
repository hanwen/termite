package stats

import (
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"
)

var _ = log.Println

type ServerStats struct {
	mutex       sync.Mutex
	phaseCounts map[string]int
	*CpuStatSampler
	*DiskStatSampler
	PhaseOrder []string
}

func NewServerStats() *ServerStats {
	return &ServerStats{
		phaseCounts:     map[string]int{},
		CpuStatSampler:  NewCpuStatSampler(),
		DiskStatSampler: NewDiskStatSampler(),
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

func CpuStatsWriteHttp(w http.ResponseWriter, stats []CpuStat, disk []DiskStat) {
	if len(stats) == 0 {
		return
	}
	if len(stats) != len(disk) {
		return
	}
	statm := CpuStat{}
	stat5 := CpuStat{}

	diskm := DiskStat{}
	disk5 := DiskStat{}

	count5 := int64(0)
	for i, v := range stats {
		statm = statm.Add(v)
		diskm.Add(&disk[i])
		if i >= len(stats)-5 {
			count5++
			stat5 = stat5.Add(v)

			disk5.Add(&disk[i])
		}
	}

	printChild := statm.ChildCpu+statm.ChildSys > 0
	chHeader := ""
	if printChild {
		chHeader = "<th>child cpu (ms)</th><th>child sys (ms)</th>"
	}
	fmt.Fprintf(w, "<p><table><tr><th>self cpu (ms)</th><th>self sys (ms)</th>%s<th>total (ms)</th><th>read ops</th><th>write ops</th></tr>",
		chHeader)
	for i, v := range stats {
		if i < len(stats)-5 {
			continue
		}

		chRow := ""
		if printChild {
			chRow = fmt.Sprintf("<td>%d</td><td>%d</td>",
				v.ChildCpu/time.Millisecond, v.ChildSys/time.Millisecond)
		}

		fmt.Fprintf(w, "<tr><td>%d</td><td>%d</td>%s<td>%d</td><td>%d</td><td>%d</td></tr>",
			v.SelfCpu/time.Millisecond, v.SelfSys/time.Millisecond, chRow,
			(v.Total())/time.Millisecond,
			disk[i].ReadsCompleted,
			disk[i].WritesCompleted)
	}
	fmt.Fprintf(w, "</table>")

	fmt.Fprintf(w, "<p>CPU (last min): %d s self %d s sys", statm.SelfCpu/time.Second, statm.SelfSys/time.Second)
	if printChild {
		fmt.Fprintf(w, " %d s child %d s sys", statm.ChildCpu/time.Second, statm.ChildSys/time.Second)
	}

	fmt.Fprintf(w, " %.2f CPU", float64(statm.Total())/1e9/float64(len(stats)))
	fmt.Fprintf(w, "<p>CPU (last %ds): %.2f self %.2f sys",
		count5, float64(stat5.SelfCpu)/float64(time.Second), float64(stat5.SelfSys)/float64(time.Second))
	if printChild {
		fmt.Fprintf(w, "%.2f s child %.2f s sys",
			float64(stat5.ChildCpu)/float64(time.Second),
			float64(stat5.ChildSys)/float64(time.Second))
	}
	fmt.Fprintf(w, " %.2f CPU", float64(stat5.Total())/1e9/float64(count5))
	fmt.Fprintf(w, "<p>Disk (last %ds): Read %.2f/s Write %.2f/s",
		count5,
		float64(disk5.MergedReadsCompleted)/(float64(count5)*float64(time.Second)),
		float64(disk5.WritesCompleted)/(float64(5)*float64(time.Second)))
}

func CountStatsWriteHttp(w http.ResponseWriter, names []string, counts []int) {
	fmt.Fprintf(w, "<ul>")
	for i, c := range counts {
		fmt.Fprintf(w, "<li>Jobs in phase %s: %d ", names[i], c)
	}
	fmt.Fprintf(w, "</ul>")
}

func (me *ServerStats) WriteHttp(w http.ResponseWriter) {
	cpu := me.CpuStats()
	disk := me.DiskStats()

	l := len(cpu)
	if len(disk) < l {
		l = len(disk)
	}
	CpuStatsWriteHttp(w, cpu[:l], disk[:l])
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
