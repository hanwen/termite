package termite

import (
	"github.com/hanwen/termite/stats"
)

func (me *Mirror) Status(req *MirrorStatusRequest, rep *MirrorStatusResponse) error {
	me.fsMutex.Lock()
	defer me.fsMutex.Unlock()
	rep.Root = me.writableRoot
	rep.Granted = me.maxJobCount
	rep.WaitingTasks = me.waiting
	rep.Accepting = me.accepting

	for fs := range me.activeFses {
		rep.Fses = append(rep.Fses, fs.Status())
	}
	rep.RpcTimings = me.rpcFs.timings.TimingMessages()
	return nil
}

func (me *Worker) Status(req *WorkerStatusRequest, rep *WorkerStatusResponse) error {
	me.mirrors.Status(req, rep)

	// TODO - pass WorkerOptions out.
	rep.MaxJobCount = me.options.Jobs
	rep.Version = Version()
	rep.Accepting = me.accepting
	rep.CpuStats = me.stats.CpuStats()
	rep.PhaseCounts = me.stats.PhaseCounts()
	rep.PhaseNames = me.stats.PhaseOrder
	rep.TotalCpu = *stats.TotalCpuStat()
	rep.ContentCacheHitRate = me.content.MemoryHitRate()
	rep.MemStat = *stats.GetMemStat()
	return nil
}
