package termite

import (
	"fmt"
	"github.com/hanwen/termite/attr"
	"os"
)

type AttrRequest struct {
	Origin string
	Name   string
}

type AttrResponse struct {
	Attrs []*attr.FileAttr
}

type UpdateRequest struct {
	Files []*attr.FileAttr
}

type UpdateResponse struct {

}

type MirrorStatusRequest struct {

}

type RpcTiming struct {
	N  int64
	Ns int64
}

func (me *RpcTiming) String() string {
	avg := me.Ns / me.N

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

type MirrorStatusResponse struct {
	Root         string
	Granted      int
	Running      []string
	ShuttingDown bool
	WaitingTasks int
	IdleFses     int
	RpcTimings   []string
}

type WorkerStatusRequest struct {

}

type CpuStat struct {
	SelfCpu  int64
	SelfSys  int64
	ChildCpu int64
	ChildSys int64
}

type WorkerStatusResponse struct {
	MirrorStatus []MirrorStatusResponse
	Version      string
	MaxJobCount  int
	ShuttingDown bool

	// In chronological order.
	CpuStats            []CpuStat
	TotalCpu            CpuStat
	ContentCacheHitRate float64
	PhaseNames          []string        
	PhaseCounts         []int 
}

type Timing struct {
	Name string
	Dt   float64
}

type WorkResponse struct {
	Exit   os.Waitmsg
	Stderr string
	Stdout string

	Timings []Timing

	LastTime int64

	*attr.FileSet
	TaskIds  []int
	WorkerId string
}

type WorkRequest struct {
	// Id of connection streaming stdin.
	TaskId       int
	StdinId      string
	Debug        bool
	WritableRoot string
	Binary       string
	Argv         []string
	Env          []string
	Dir          string
	RanLocally   bool

	// If set, must run on worker. Used for debugging.
	Worker string
}

func (me *WorkRequest) Summary() string {
	return fmt.Sprintf("Stdin %s Cmd %s Id %d", me.StdinId, me.Argv, me.TaskId)
}

type CreateMirrorRequest struct {
	RpcId        string
	RevRpcId     string
	WritableRoot string
	// Max number of processes to reserve.
	MaxJobCount int
}

type CreateMirrorResponse struct {
	GrantedJobCount int
}

type ShutdownRequest struct {
	Restart bool
}

type ShutdownResponse struct {

}

type LogRequest struct {
	Whence int
	Off    int64
	Size   int64
}

type LogResponse struct {
	Data []byte
}
