package termite

import (
	"os"
)

func (me *Mirror) Status(req *MirrorStatusRequest, rep *MirrorStatusResponse) os.Error {
	me.fuseFileSystemsMutex.Lock()
	defer me.fuseFileSystemsMutex.Unlock()

	rep.Root = me.writableRoot
	rep.Granted = me.maxJobCount
	rep.WaitingTasks = me.Waiting
	rep.IdleFses = len(me.unusedFileSystems)
	rep.ShuttingDown = me.shuttingDown
	for fs, _ := range me.workingFileSystems {
		rep.Running = append(rep.Running, fs.task.taskInfo)
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
	return nil
}
