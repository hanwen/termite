package termite

import (
	"fmt"
	"log"
	"os"
	"syscall"
)

// TODO should be in syscall package.
const RUSAGE_SELF = 0
const RUSAGE_CHILDREN = -1

func sampleTime() interface{} {
	c := TotalCpuStat()
	return c
}

func TotalCpuStat() *CpuStat {
	c := CpuStat{}
	r := syscall.Rusage{}
	errNo := syscall.Getrusage(RUSAGE_SELF, &r)
	if errNo != 0 {
		log.Println("Getrusage:", errNo)
		return nil
	}

	c.SelfCpu = syscall.TimevalToNsec(r.Utime)
	c.SelfSys = syscall.TimevalToNsec(r.Stime)

	errNo = syscall.Getrusage(RUSAGE_CHILDREN, &r)
	if errNo != 0 {
		return nil
	}
	c.ChildCpu = syscall.TimevalToNsec(r.Utime)
	c.ChildSys = syscall.TimevalToNsec(r.Stime)

	return &c
}

func (me *CpuStat) Diff(x CpuStat) CpuStat {
	return CpuStat{
		SelfSys:  me.SelfSys - x.SelfSys,
		SelfCpu:  me.SelfCpu - x.SelfCpu,
		ChildSys: me.ChildSys - x.ChildSys,
		ChildCpu: me.ChildCpu - x.ChildCpu,
	}
}

func (me *CpuStat) Total() int64 {
	return me.SelfSys + me.SelfCpu + me.ChildSys + me.ChildCpu
}

type workerStats struct {
	cpuTimes *PeriodicSampler
}

func newWorkerStats() *workerStats {
	me := &workerStats{
		cpuTimes: NewPeriodicSampler(1.0, 10, sampleTime),
	}
	return me
}

func (me *workerStats) CpuStats() (out []CpuStat) {
	vals := me.cpuTimes.Values()
	var last *CpuStat
	for _, v := range vals {
		s := v.(*CpuStat)
		if last != nil {
			out = append(out, s.Diff(*last))
		}
		last = s
	}
	return out
}

func (me *Mirror) Status(req *MirrorStatusRequest, rep *MirrorStatusResponse) os.Error {
	me.fsMutex.Lock()
	defer me.fsMutex.Unlock()
	rep.Root = me.writableRoot
	rep.Granted = me.maxJobCount
	rep.WaitingTasks = me.Waiting
	rep.ShuttingDown = me.shuttingDown

	i := 0
	for fs, _ := range me.activeFses {
		i++
		for t, _ := range fs.tasks {
			rep.Running = append(rep.Running, fmt.Sprintf("fs %d: %s", i, t.taskInfo))
		}
	}
	return nil
}

func (me *WorkerDaemon) Status(req *WorkerStatusRequest, rep *WorkerStatusResponse) os.Error {
	me.mirrorMapMutex.Lock()
	defer me.mirrorMapMutex.Unlock()

	for _, mirror := range me.mirrorMap {
		mRep := MirrorStatusResponse{}
		mReq := MirrorStatusRequest{}
		mirror.Status(&mReq, &mRep)

		rep.MirrorStatus = append(rep.MirrorStatus, mRep)
	}
	rep.MaxJobCount = me.maxJobCount
	rep.Version = Version()
	rep.ShuttingDown = me.shuttingDown
	rep.CpuStats = me.stats.CpuStats()
	rep.TotalCpu = *TotalCpuStat()
	return nil
}
