package termite

import (
	"fmt"
	"http"
	"sync"
	"sync/atomic"
	"time"
)

type masterStats struct {
	counterMutex sync.Mutex
	received     *MultiResolutionCounter

	running int32

	workerPhaseStats map[string]float64
}

func newMasterStats() *masterStats {
	return &masterStats{
		received: NewMultiResolutionCounter(1, time.Seconds(), []int{60, 10}),
		workerPhaseStats: map[string]float64{},
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
	me.counterMutex.Lock()
	defer me.counterMutex.Unlock()
	me.received.Add(time.Seconds(), 0)
	fmt.Fprintf(w, "<p>Received (sec/min/10min): %v", me.received.Read())

	r := atomic.AddInt32(&me.running, 0)
	fmt.Fprintf(w, "<p>Jobs in receive status: %d "+
		"(parallelism of the job)", r)

	fmt.Fprintf(w, "<p>Request phases:<ul>")
	for k, v := range me.workerPhaseStats {
		fmt.Fprintf(w, "<li>%s: %.1f ms\n", k, v)
	}
	fmt.Fprintf(w, "</ul>")
}
