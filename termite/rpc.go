package termite

import (
	"fmt"
	"github.com/hanwen/termite/attr"
	"github.com/hanwen/termite/stats"
	"os"
)

type AttrRequest struct {
	Name string

	// Worker asking for the request. Useful for debugging.
	Origin string
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

type FuseFsStatus struct {
	Id    string
	Tasks []string
	Mem   string
}

type MirrorStatusResponse struct {
	Root         string
	Granted      int
	Fses         []FuseFsStatus
	Accepting    bool
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
	Accepting    bool

	// In chronological order.
	CpuStats            []stats.CpuStat
	TotalCpu            stats.CpuStat
	ContentCacheHitRate float64
	ContentCacheHitAge  int

	PhaseNames  []string
	PhaseCounts []int
	MemStat     stats.MemStat
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

	// Reaped files, if any
	*attr.FileSet

	// Task ids for which the fileset contains data.
	TaskIds []int

	// Worker where this was processed.
	WorkerId string
}

type WorkRequest struct {
	// Unique id of this request.
	TaskId int

	// Id of connection streaming stdin.
	StdinId string
	Debug   bool
	Binary  string
	Argv    []string
	Env     []string
	Dir     string

	// Signal that a command ran locally.  Used for logging in the master.
	RanLocally bool

	// If set, must run on this worker. Used for debugging.
	Worker string
}

func (me *WorkRequest) Summary() string {
	return fmt.Sprintf("Stdin %s Cmd %s Id %d", me.StdinId, me.Argv, me.TaskId)
}

type CreateMirrorRequest struct {
	// Ids of connections to use for RPC
	RpcId        string
	RevRpcId     string
	ContentId    string
	RevContentId string

	// The writable root for the mirror.
	WritableRoot string

	// Max number of processes to reserve.
	MaxJobCount int
}

type CreateMirrorResponse struct {
	GrantedJobCount int
}

type ShutdownRequest struct {
	Restart bool
	Kill    bool
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
