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
	wm := &WorkerMirrors{
		worker:    w,
		mirrorMap: make(map[string]*Mirror),
	}
	wm.cond = sync.NewCond(&wm.mirrorMapMutex)
	return wm
}

func (wm *WorkerMirrors) getMirror(rpcConn, revConn, contentConn, revContentConn io.ReadWriteCloser, reserveCount int, writableRoot string) (*Mirror, error) {
	if reserveCount <= 0 {
		return nil, errors.New("must ask positive jobcount")
	}
	wm.mirrorMapMutex.Lock()
	defer wm.mirrorMapMutex.Unlock()
	used := 0
	for _, v := range wm.mirrorMap {
		used += v.maxJobCount
	}

	remaining := wm.worker.options.Jobs - used
	if remaining <= 0 {
		return nil, errors.New("no processes available")
	}
	if remaining < reserveCount {
		reserveCount = remaining
	}

	mirror, err := NewMirror(wm.worker, rpcConn, revConn, contentConn, revContentConn)
	if err != nil {
		return nil, err
	}

	if err := mirror.startFUSE(writableRoot); err != nil {
		return nil, err
	}

	mirror.maxJobCount = reserveCount

	key := fmt.Sprintf("todo%d", rand.Int63n(1<<60))
	wm.mirrorMap[key] = mirror
	mirror.key = key
	return mirror, nil
}

func (wm *WorkerMirrors) DropMirror(mirror *Mirror) {
	wm.mirrorMapMutex.Lock()
	defer wm.mirrorMapMutex.Unlock()

	log.Println("dropping mirror", mirror.key)
	delete(wm.mirrorMap, mirror.key)
	wm.cond.Broadcast()
	runtime.GC()
}

func (wm *WorkerMirrors) mirrors() (result []*Mirror) {
	wm.mirrorMapMutex.Lock()
	defer wm.mirrorMapMutex.Unlock()
	for _, m := range wm.mirrorMap {
		result = append(result, m)
	}
	return
}

func (wm *WorkerMirrors) shutdown(aggressive bool) {
	log.Printf("shutting down mirrors: aggressive=%v", aggressive)
	wg := sync.WaitGroup{}
	mirrors := wm.mirrors()
	wg.Add(len(mirrors))
	for _, m := range mirrors {
		go func(m *Mirror) {
			m.shutdown(aggressive)
			wg.Done()
		}(m)
	}

	wg.Wait()
	wm.mirrorMap = map[string]*Mirror{}
	log.Println("All mirrors have shut down.")
}

func (wm *WorkerMirrors) Status(req *WorkerStatusRequest, rep *WorkerStatusResponse) {
	for _, mirror := range wm.mirrors() {
		mRep := MirrorStatusResponse{}
		mReq := MirrorStatusRequest{}
		mirror.Status(&mReq, &mRep)

		rep.MirrorStatus = append(rep.MirrorStatus, mRep)
	}
}
