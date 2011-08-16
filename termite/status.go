package termite

import (
	"os"
)
	
type MirrorStatusRequest struct {
}

type MirrorStatusResponse struct {
	Root string
	Granted int
	Running []string
	ShuttingDown bool
	WaitingTasks int
	IdleFses int
}


func (me *Mirror) Status(req *MirrorStatusRequest, rep *MirrorStatusResponse) os.Error {
	me.fuseFileSystemsMutex.Lock()
	defer me.fuseFileSystemsMutex.Unlock()

	rep.Root = me.writableRoot
	rep.Granted = me.maxJobCount
	rep.WaitingTasks = me.Waiting
	rep.IdleFses = len(me.fuseFileSystems)
	rep.ShuttingDown = me.shuttingDown 
	for _, v := range me.workingFileSystems {
		rep.Running = append(rep.Running, v)
	}
	return nil
}

type WorkerStatusRequest struct {

}

type WorkerStatusResponse struct {
	MirrorStatus []MirrorStatusResponse
	Version      string
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

	rep.Version = Version()
	return nil
}
