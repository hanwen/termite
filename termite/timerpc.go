package termite

import (
	"fmt"
	"rpc"
	"sort"
	"sync"
	"time"
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
	me.mu.Lock()
	defer me.mu.Unlock()
	timing := me.timings[name]
	if timing == nil {
		timing = new(RpcTiming)
		me.timings[name] = timing
	}

	timing.N++
	timing.Ns += dt
}

func (me *TimedRpcClient) Call(serviceMethod string, args interface{}, reply interface{}) error {
	start := time.Nanoseconds()
	err := me.Client.Call(serviceMethod, args, reply)
	dt := time.Nanoseconds() - start
	me.Log(serviceMethod, dt)
	return err
}

func NewTimedRpcClient(cl *rpc.Client) *TimedRpcClient {
	return &TimedRpcClient{
		Client:  cl,
		TimerStats: NewTimerStats(),
	}
}

type TimedRpcClient struct {
	*TimerStats
	*rpc.Client
}
