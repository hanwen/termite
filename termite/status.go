package termite

import "fmt"

func (me *Mirror) Status(req *MirrorStatusRequest, rep *MirrorStatusResponse) error {
	me.fsMutex.Lock()
	defer me.fsMutex.Unlock()
	rep.Root = me.writableRoot
	rep.Granted = me.maxJobCount
	rep.WaitingTasks = me.waiting
	rep.ShuttingDown = me.shuttingDown

	for fs := range me.activeFses {
		for t := range fs.tasks {
			rep.Running = append(rep.Running, fmt.Sprintf("fs %s: %s", fs.id, t.taskInfo))
		}
	}
	rep.RpcTimings = me.rpcFs.timings.TimingMessages()
	return nil
}

func (me *Worker) Status(req *WorkerStatusRequest, rep *WorkerStatusResponse) error {
	me.mirrors.Status(req, rep)

	// TODO - pass WorkerOptions out.
	rep.MaxJobCount = me.options.Jobs
	rep.Version = Version()
	rep.ShuttingDown = me.shuttingDown
	me.stats.FillWorkerStatus(rep)
	rep.TotalCpu = *TotalCpuStat()
	rep.ContentCacheHitRate = me.contentCache.MemoryHitRate()
	return nil
}
