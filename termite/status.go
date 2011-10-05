package termite

import (
	"fmt"
	"os"
)

func (me *Mirror) Status(req *MirrorStatusRequest, rep *MirrorStatusResponse) os.Error {
	me.fsMutex.Lock()
	defer me.fsMutex.Unlock()
	rep.Root = me.writableRoot
	rep.Granted = me.maxJobCount
	rep.WaitingTasks = me.waiting
	rep.ShuttingDown = me.shuttingDown

	i := 0
	for fs := range me.activeFses {
		i++
		for t := range fs.tasks {
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
