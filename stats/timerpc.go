package stats

import (
	"fmt"
	"sort"
	"sync"
	"time"
)

type TimerStats struct {
	mu      sync.Mutex
	timings map[string]*RpcTiming
}

type RpcTiming struct {
	N int64
	time.Duration
}

func (me *RpcTiming) String() string {
	avg := int64(me.Duration) / me.N

	unit := "ns"
	div := int64(1)
	switch {
	case avg > 1e9:
		unit = "s"
		div = 1e9
	case avg > 1e6:
		unit = "ms"
		div = 1e6
	case avg > 1e3:
		unit = "us"
		div = 1e3
	}
	return fmt.Sprintf("%d calls, %d %s/call", me.N, avg/div, unit)
}

func NewTimerStats() *TimerStats {
	return &TimerStats{
		timings: map[string]*RpcTiming{},
	}
}

func (me *TimerStats) TimingMessages() []string {
	t := me.Timings()
	names := []string{}
	for n := range t {
		names = append(names, n)
	}
	sort.Strings(names)

	msgs := []string{}
	for _, n := range names {
		v := t[n]
		msgs = append(msgs, fmt.Sprintf("%s: %s", n, v.String()))
	}
	return msgs
}

func (me *TimerStats) Timings() map[string]*RpcTiming {
	me.mu.Lock()
	defer me.mu.Unlock()
	result := map[string]*RpcTiming{}
	for n, r := range me.timings {
		r2 := *r
		result[n] = &r2
	}
	return result
}

func (me *TimerStats) Log(name string, dt time.Duration) {
	me.LogN(name, 1, dt)
}

func (me *TimerStats) LogN(name string, n int64, dt time.Duration) {
	me.mu.Lock()
	defer me.mu.Unlock()
	timing := me.timings[name]
	if timing == nil {
		timing = new(RpcTiming)
		me.timings[name] = timing
	}

	timing.N += n
	timing.Duration += dt
}
