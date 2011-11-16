package termite

import (
	"errors"
	"fmt"
	"log"
	"net"
	"runtime"
	"sync"
)

type WorkerMirrors struct {
	worker         *Worker
	mirrorMapMutex sync.Mutex
	cond           *sync.Cond
	mirrorMap      map[string]*Mirror
}

func NewWorkerMirrors(w *Worker) *WorkerMirrors {
	me := &WorkerMirrors{
		worker:    w,
		mirrorMap: make(map[string]*Mirror),
	}
	me.cond = sync.NewCond(&me.mirrorMapMutex)
	return me
}

func (me *WorkerMirrors) getMirror(rpcConn, revConn net.Conn, reserveCount int) (*Mirror, error) {
	if reserveCount <= 0 {
		return nil, errors.New("must ask positive jobcount")
	}
	me.mirrorMapMutex.Lock()
	defer me.mirrorMapMutex.Unlock()
	used := 0
	for _, v := range me.mirrorMap {
		used += v.maxJobCount
	}

	remaining := me.worker.options.Jobs - used
	if remaining <= 0 {
		return nil, errors.New("no processes available")
	}
	if remaining < reserveCount {
		reserveCount = remaining
	}

	mirror := NewMirror(me.worker, rpcConn, revConn)
	mirror.maxJobCount = reserveCount
	key := fmt.Sprintf("%v", rpcConn.RemoteAddr())
	me.mirrorMap[key] = mirror
	mirror.key = key
	return mirror, nil
}

func (me *WorkerMirrors) DropMirror(mirror *Mirror) {
	me.mirrorMapMutex.Lock()
	defer me.mirrorMapMutex.Unlock()

	log.Println("dropping mirror", mirror.key)
	delete(me.mirrorMap, mirror.key)
	me.cond.Broadcast()
	runtime.GC()
}

func (me *WorkerMirrors) Shutdown(req *ShutdownRequest) {
	log.Println("Received Shutdown.")
	me.mirrorMapMutex.Lock()
	defer me.mirrorMapMutex.Unlock()

	for _, m := range me.mirrorMap {
		m.Shutdown()
	}
	log.Println("Asked all mirrors to shut down.")
	for len(me.mirrorMap) > 0 {
		log.Println("Live mirror count:", len(me.mirrorMap))
		me.cond.Wait()
	}
	log.Println("All mirrors have shut down.")
}

func (me *WorkerMirrors) Status(req *WorkerStatusRequest, rep *WorkerStatusResponse) {
	me.mirrorMapMutex.Lock()
	defer me.mirrorMapMutex.Unlock()

	for _, mirror := range me.mirrorMap {
		mRep := MirrorStatusResponse{}
		mReq := MirrorStatusRequest{}
		mirror.Status(&mReq, &mRep)

		rep.MirrorStatus = append(rep.MirrorStatus, mRep)
	}
}
