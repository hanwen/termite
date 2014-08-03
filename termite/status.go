package termite

import (
	"github.com/hanwen/termite/stats"
)

func (m *Mirror) Status(req *MirrorStatusRequest, rep *MirrorStatusResponse) error {
	m.fsMutex.Lock()
	defer m.fsMutex.Unlock()
	rep.Root = m.writableRoot
	rep.Granted = m.maxJobCount
	rep.WaitingTasks = m.waiting
	rep.Accepting = m.accepting

	for fs := range m.activeFses {
		rep.Fses = append(rep.Fses, fs.Status())
	}
	rep.RpcTimings = append(m.rpcFs.timings.TimingMessages(),
		m.worker.content.TimingMessages()...)
	return nil
}

func (w *Worker) Status(req *WorkerStatusRequest, rep *WorkerStatusResponse) error {
	w.mirrors.Status(req, rep)

	// TODO - pass WorkerOptions out.
	rep.MaxJobCount = w.options.Jobs
	rep.Version = Version()
	rep.Accepting = w.accepting
	rep.CpuStats = w.stats.CpuStats()
	rep.DiskStats = w.stats.DiskStats()
	rep.PhaseCounts = w.stats.PhaseCounts()
	rep.PhaseNames = w.stats.PhaseOrder
	rep.TotalCpu = *stats.TotalCpuStat()
	rep.MemStat = *stats.GetMemStat()
	return nil
}
