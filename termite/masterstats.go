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
}

func newMasterStats() *masterStats {
	return &masterStats{
		received: NewMultiResolutionCounter(1, time.Seconds(), []int{60, 10}),
	}
}

func (me *masterStats) MarkReceive() {
	me.counterMutex.Lock()
	defer me.counterMutex.Unlock()
	me.received.Add(time.Seconds(), 1)
	atomic.AddInt32(&me.running, 1)
}

func (me *masterStats) MarkReturn() {
	atomic.AddInt32(&me.running, -1)
}

func (me *masterStats) writeHttp(w http.ResponseWriter) {
	me.counterMutex.Lock()
	defer me.counterMutex.Unlock()
	me.received.Add(time.Seconds(), 0)
	fmt.Fprintf(w, "<p>Received (sec/min/10min): %v", me.received.Read())

	r := atomic.AddInt32(&me.running, 0)
	fmt.Fprintf(w, "<p>Jobs in receive status: %d "+
		"(parallelism of the job)", r)
}
