package termite
import (
	"os"
	"rpc"
	"sync"
	"time"
)

func (me *TimedRpcClient) Call(serviceMethod string, args interface{}, reply interface{}) os.Error {
	start := time.Nanoseconds()
	err := me.Client.Call(serviceMethod, args, reply)
	dt := time.Nanoseconds() - start

	me.mu.Lock()
	defer me.mu.Unlock()
	timing := me.timings[serviceMethod]
	if timing == nil {
		timing = new(RpcTiming)
		me.timings[serviceMethod] = timing
	}

	timing.N++
	timing.Ns += dt

	return err
}

func (me *TimedRpcClient) Timings() map[string]*RpcTiming {
	me.mu.Lock()
	defer me.mu.Unlock()
	result := map[string]*RpcTiming{}
	for n, r := range me.timings {
		r2 := *r
		result[n] = &r2
	}
	return result
}

func NewTimedRpcClient(cl *rpc.Client) *TimedRpcClient {
	return &TimedRpcClient{
		Client: cl,
		timings: map[string]*RpcTiming{},
	}
}

type TimedRpcClient struct {
	mu sync.Mutex
	timings map[string]*RpcTiming
	
	*rpc.Client
}
