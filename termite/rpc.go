package termite

import (
	"fmt"
	"io"
	"syscall"

	"github.com/hanwen/termite/attr"
	"github.com/hanwen/termite/stats"
)

type Empty struct{}

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
	CpuStats  []stats.CpuStat
	TotalCpu  stats.CpuStat
	DiskStats []stats.DiskStat

	PhaseNames  []string
	PhaseCounts []int
	MemStat     stats.MemStat
}

type Timing struct {
	Name string
	Dt   float64
}

type WorkResponse struct {
	Exit   syscall.WaitStatus
	Stderr string
	Stdout string

	Timings []Timing

	// Reaped files, if any
	*attr.FileSet

	// Task ids for which the fileset contains data.
	TaskIds []int

	// Files from the backing store that were read.
	Reads []string

	// Worker where this was processed.
	WorkerId string
}

type WorkRequest struct {
	// Unique id of this request.
	TaskId int

	// Id of connection streaming stdin.
	StdinId string

	// TODO - don't abuse RPC message for transporting this.
	StdinConn io.ReadWriteCloser

	Debug  bool
	Binary string
	Argv   []string
	Env    []string
	Dir    string

	// Signal that a command ran locally.  Used for logging in the master.
	RanLocally bool

	// if TrackReads is set, the worker will return files read by the task.
	TrackReads bool

	// If set, must run on this worker. Used for debugging.
	Worker string
}

func (r *WorkRequest) Summary() string {
	return fmt.Sprintf("Stdin %s Cmd %s Id %d", r.StdinId, r.Argv, r.TaskId)
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
