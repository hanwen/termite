package termite

import (
	"fmt"
	"io"
	"log"
	"net"
	"net/rpc"
	"sync"

	"github.com/hanwen/termite/attr"
)

// State associated with one master.
type Mirror struct {
	worker      *Worker
	rpcConn     io.ReadWriteCloser
	contentConn io.ReadWriteCloser

	rpcFs        *RpcFs
	writableRoot string

	// key in Worker's map.
	key string

	maxJobCount int

	fsMutex    sync.Mutex
	cond       *sync.Cond
	waiting    int
	nextFsId   int
	activeFses map[*fuseFS]bool
	accepting  bool
	killed     bool
}

func NewMirror(worker *Worker, rpcConn, revConn, contentConn, revContentConn io.ReadWriteCloser) *Mirror {
	mirror := &Mirror{
		activeFses:  map[*fuseFS]bool{},
		rpcConn:     rpcConn,
		contentConn: contentConn,
		worker:      worker,
		accepting:   true,
	}
	_, portString, _ := net.SplitHostPort(worker.listener.Addr().String())
	id := Hostname + ":" + portString
	mirror.cond = sync.NewCond(&mirror.fsMutex)
	attrClient := attr.NewClient(revConn, id)
	mirror.rpcFs = NewRpcFs(attrClient, worker.content, revContentConn)
	mirror.rpcFs.id = id
	mirror.rpcFs.attr.Paranoia = worker.options.Paranoia

	go mirror.serveRpc()
	return mirror
}

func (me *Mirror) serveRpc() {
	server := rpc.NewServer()
	server.Register(me)
	done := make(chan int, 2)
	go func() {
		server.ServeConn(me.rpcConn)
		done <- 1
	}()
	go func() {
		me.worker.content.ServeConn(me.contentConn)
		done <- 1
	}()
	<-done
	me.Shutdown(true)
	me.worker.DropMirror(me)
}

func (me *Mirror) Shutdown(aggressive bool) {
	me.fsMutex.Lock()
	defer me.fsMutex.Unlock()
	me.accepting = false
	if aggressive {
		me.killed = true
	}

	for fs := range me.activeFses {
		if len(fs.tasks) == 0 {
			fs.Stop()
			delete(me.activeFses, fs)
		}
	}

	if aggressive {
		me.rpcFs.Close()
		for fs := range me.activeFses {
			for t := range fs.tasks {
				t.Kill()
			}
		}
	}
	for len(me.activeFses) > 0 {
		me.cond.Wait()
	}

	me.rpcConn.Close()
	me.contentConn.Close()
}

func (me *Mirror) runningCount() int {
	r := 0
	for fs := range me.activeFses {
		r += len(fs.tasks)
	}
	return r
}

var ShuttingDownError error

func init() {
	ShuttingDownError = fmt.Errorf("shutting down")
}

func (me *Mirror) newFs(t *WorkerTask) (fs *fuseFS, err error) {
	me.fsMutex.Lock()
	defer me.fsMutex.Unlock()

	me.waiting++
	for me.runningCount() >= me.maxJobCount {
		me.cond.Wait()
	}
	me.waiting--

	if !me.accepting {
		return nil, ShuttingDownError
	}

	for fs := range me.activeFses {
		if !fs.reaping && len(fs.taskIds) < me.worker.options.ReapCount {
			fs.addTask(t)
			return fs, nil
		}
	}
	fs, err = me.newWorkerFuseFs()
	if err != nil {
		return nil, err
	}

	me.prepareFS(fs)
	fs.addTask(t)
	me.activeFses[fs] = true
	return fs, nil
}

// Must hold lock.
func (me *Mirror) prepareFS(fs *fuseFS) {
	fs.reaping = false
	fs.taskIds = make([]int, 0, me.worker.options.ReapCount)
}

func (me *Mirror) considerReap(fs *fuseFS, task *WorkerTask) bool {
	me.fsMutex.Lock()
	defer me.fsMutex.Unlock()
	delete(fs.tasks, task)
	if len(fs.tasks) == 0 {
		fs.reaping = true
	}
	me.cond.Broadcast()
	return fs.reaping
}

func (me *Mirror) reapFuse(fs *fuseFS) (results *attr.FileSet, taskIds []int) {
	log.Printf("Reaping fuse FS %v", fs.id)
	ids := fs.taskIds[:]
	results = me.fillReply(fs)

	return results, ids
}

func (me *Mirror) returnFs(fs *fuseFS) {
	me.fsMutex.Lock()
	defer me.fsMutex.Unlock()

	if fs.reaping {
		me.prepareFS(fs)
	}

	fs.SetDebug(false)
	if !me.accepting {
		fs.Stop()
		delete(me.activeFses, fs)
		me.cond.Broadcast()
	}
}

func (me *Mirror) Update(req *UpdateRequest, rep *UpdateResponse) error {
	me.updateFiles(req.Files)
	return nil
}

func (me *Mirror) updateFiles(attrs []*attr.FileAttr) {
	me.rpcFs.updateFiles(attrs)

	me.fsMutex.Lock()
	defer me.fsMutex.Unlock()

	for fs := range me.activeFses {
		fs.update(attrs)
	}
}

func (me *Mirror) Run(req *WorkRequest, rep *WorkResponse) error {
	me.worker.stats.Enter("run")

	// Don't run me.updateFiles() as we don't want to issue
	// unneeded cache invalidations.
	task, err := me.newWorkerTask(req, rep)
	if err != nil {
		return err
	}

	err = task.Run()
	if err != nil {
		log.Println("task.Run:", err)
		return err
	}

	log.Println(rep)
	rep.WorkerId = fmt.Sprintf("%s: %s", Hostname, me.worker.listener.Addr().String())
	me.worker.stats.Exit("run")

	if me.killed {
		return fmt.Errorf("killed worker %s", me.worker.listener.Addr().String())
	}
	return nil
}

const _DELETIONS = "DELETIONS"

func (me *Mirror) newWorkerFuseFs() (*fuseFS, error) {
	f, err := newFuseFS(me.worker.options.TempDir, me.rpcFs, me.writableRoot,
		me.worker.options.User)
	if err != nil {
		return nil, err
	}
	f.id = fmt.Sprintf("%d", me.nextFsId)
	me.nextFsId++

	return f, err
}

func (me *Mirror) newWorkerTask(req *WorkRequest, rep *WorkResponse) (*WorkerTask, error) {
	var stdin io.ReadWriteCloser
	if req.StdinId != "" {
		stdin = me.worker.listener.Accept(req.StdinId)
	}
	task := &WorkerTask{
		req:       req,
		rep:       rep,
		stdinConn: stdin,
		mirror:    me,
		taskInfo:  fmt.Sprintf("%v, dir %v", req.Argv, req.Dir),
	}
	return task, nil
}
