package termite

import (
	"fmt"
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
	Hash string
	Link string

	// Only filled for directories.
	NameModeMap map[string]uint32
}

func (me FileAttr) String() string {
	id := me.Path
	if me.Hash != "" {
		id += fmt.Sprintf(" sz %d", me.FileInfo.Size)
	}
	if me.Link != "" {
		id += fmt.Sprintf(" -> %s", me.Link)
	}
	if me.FileInfo != nil {
		id += fmt.Sprintf(" m=%o", me.FileInfo.Mode)
	} else {
		id += " (del)"
	}
	return id
}

type AttrResponse struct {
	Attrs []*FileAttr
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

	// In chronological order.
	CpuStats []CpuStat
	TotalCpu CpuStat
}

type Timing struct {
	Name string
	Dt   float64
}

type FileSet struct {
	Files []*FileAttr
}

func (me *FileSet) String() string {
	return fmt.Sprintf("%v", me.Files)
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

type ShutdownRequest struct {
	Restart bool
}

type ShutdownResponse struct {

}
