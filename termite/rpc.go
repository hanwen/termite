package termite

import (
	"fmt"
	"github.com/hanwen/termite/attr"
	"github.com/hanwen/termite/stats"
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

type WorkerStatusResponse struct {
	MirrorStatus []MirrorStatusResponse
	Version      string
	MaxJobCount  int
	ShuttingDown bool

	// In chronological order.
	CpuStats            []stats.CpuStat
	TotalCpu            stats.CpuStat
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
