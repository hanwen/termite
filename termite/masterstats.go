package termite

import (
	"fmt"
	"http"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

var _ = log.Println

type masterStats struct {
	counterMutex sync.Mutex
	received     *MultiResolutionCounter

	running int32

	workerPhaseStats map[string]float64
	cpuStats         *cpuStatSampler
}

func newMasterStats() *masterStats {
	return &masterStats{
		received:         NewMultiResolutionCounter(1, time.Seconds(), []int{60, 10}),
		workerPhaseStats: map[string]float64{},
		cpuStats:         newCpuStatSampler(),
	}
}

func (me *masterStats) MarkReceive() {
	me.counterMutex.Lock()
	defer me.counterMutex.Unlock()
	me.received.Add(time.Seconds(), 1)
	atomic.AddInt32(&me.running, 1)
}

func (me *masterStats) MarkReturn(resp *WorkResponse) {
	me.counterMutex.Lock()
	defer me.counterMutex.Unlock()
	atomic.AddInt32(&me.running, -1)
	for _, t := range resp.Timings {
		me.workerPhaseStats[t.Name] += t.Dt
	}
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
		
		totalm := float64(minuteStat.SelfCpu+minuteStat.SelfSys)/1.0e9
		fmt.Fprintf(w, "<p>CPU (last min): %d s self %d s sys, %.2f CPU",
			minuteStat.SelfCpu/1e9, minuteStat.SelfSys/1e9, totalm / float64(len(stats)))
		fmt.Fprintf(w, "<p>CPU (last 5s): %.2f self %.2f sys, %.1f CPU",
			float64(fiveSecStat.SelfCpu)*1e-9, float64(fiveSecStat.SelfSys)*1e-9,
			float64(fiveSecStat.SelfCpu+fiveSecStat.SelfSys)/float64(s*1e9))

	}
	me.counterMutex.Lock()
	defer me.counterMutex.Unlock()
	me.received.Add(time.Seconds(), 0)
	fmt.Fprintf(w, "<p>Received (sec/min/10min): %v", me.received.Read())

	r := atomic.AddInt32(&me.running, 0)
	fmt.Fprintf(w, "<p>Jobs in receive status: %d "+
		"(parallelism of the job)", r)

	fmt.Fprintf(w, "<p>Request phases:<ul>")
	for k, v := range me.workerPhaseStats {
		fmt.Fprintf(w, "<li>%s: %.03f s\n", k, float64(v)*1e-3)
	}
	fmt.Fprintf(w, "</ul>")
}
