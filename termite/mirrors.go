package termite

import (
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand"
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

func (me *WorkerMirrors) getMirror(rpcConn, revConn, contentConn, revContentConn io.ReadWriteCloser, reserveCount int) (*Mirror, error) {
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

	mirror := NewMirror(me.worker, rpcConn, revConn, contentConn, revContentConn)
	mirror.maxJobCount = reserveCount

	key := fmt.Sprintf("todo%d", rand.Int63n(1<<60))
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

func (me *WorkerMirrors) mirrors() (result []*Mirror) {
	me.mirrorMapMutex.Lock()
	defer me.mirrorMapMutex.Unlock()
	for _, m := range me.mirrorMap {
		result = append(result, m)
	}
	return
}

func (me *WorkerMirrors) shutdown(aggressive bool) {
	log.Printf("shutting down mirrors: aggressive=%v", aggressive)
	wg := sync.WaitGroup{}
	mirrors := me.mirrors()
	wg.Add(len(mirrors))
	for _, m := range mirrors {
		go func(m *Mirror) {
			m.Shutdown(aggressive)
			wg.Done()
		}(m)
	}

	wg.Wait()
	me.mirrorMap = map[string]*Mirror{}
	log.Println("All mirrors have shut down.")
}

func (me *WorkerMirrors) Status(req *WorkerStatusRequest, rep *WorkerStatusResponse) {
	for _, mirror := range me.mirrors() {
		mRep := MirrorStatusResponse{}
		mReq := MirrorStatusRequest{}
		mirror.Status(&mReq, &mRep)

		rep.MirrorStatus = append(rep.MirrorStatus, mRep)
	}
}
