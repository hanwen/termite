package termite

import (
	"fmt"
	"github.com/hanwen/go-fuse/fuse"
	"os"
)

type ContentRequest struct {
	Hash       string
	Start, End int
}

func (me *ContentRequest) String() string {
	return fmt.Sprintf("%x [%d, %d]", me.Hash, me.Start, me.End)
}

type ContentResponse struct {
	Chunk []byte
}

type AttrRequest struct {
	Name string
}

type FileAttr struct {
	Path string
	*os.FileInfo
	fuse.Status
	Hash string
	Link string
}

type AttrResponse struct {
	Attrs []*FileAttr
}

type DirRequest struct {
	Name string
}

type DirResponse struct {
	NameModeMap map[string]uint32
	fuse.Status
}

type UpdateRequest struct {
	Files []*FileAttr
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
	CpuStats     []CpuStat
	TotalCpu     CpuStat
}

type Timing struct {
	Name string
	Dt   float64
}

type FileSet struct {
	Files  []*FileAttr
}

type WorkResponse struct {
	Exit   os.Waitmsg
	Stderr string
	Stdout string

	Timings []Timing

	LastTime int64

	*FileSet
	FileSetId int
}

type WorkRequest struct {
	Prefetch []*FileAttr

	// Id of connection streaming stdin.
	StdinId      string
	Debug        bool
	WritableRoot string
	Binary       string
	Argv         []string
	Env          []string
	Dir          string
	RanLocally   bool
}

func (me *WorkRequest) Summary() string {
	return fmt.Sprintf("stdin %s cmd %s", me.StdinId, me.Argv)
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
