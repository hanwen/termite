package termite

import (
	"fmt"
	"sort"
	"sync"
)

type TimerStats struct {
	mu      sync.Mutex
	timings map[string]*RpcTiming
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

func (me *TimerStats) Log(name string, dt int64) {
	me.LogN(name, 1, dt)
}

func (me *TimerStats) LogN(name string, n int64, dt int64) {
	me.mu.Lock()
	defer me.mu.Unlock()
	timing := me.timings[name]
	if timing == nil {
		timing = new(RpcTiming)
		me.timings[name] = timing
	}

	timing.N += n
	timing.Ns += dt
}
