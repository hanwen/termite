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

	rpcFs *RpcFs

	fuseFS *fuseFS

	// key in Worker's map.
	key string

	maxJobCount int

	fsMutex    sync.Mutex
	cond       *sync.Cond
	waiting    int
	nextFsId   int
	activeFses map[*workerFSState]bool
	accepting  bool
	killed     bool
}

func NewMirror(worker *Worker, rpcConn, revConn, contentConn, revContentConn io.ReadWriteCloser) (*Mirror, error) {
	mirror := &Mirror{
		activeFses:  map[*workerFSState]bool{},
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
	return mirror, nil
}

func (m *Mirror) startFUSE(writableRoot string) error {
	if fs, err := newFuseFS(m.worker.options.TempDir, m.rpcFs, writableRoot); err != nil {
		return err
	} else {
		m.fuseFS = fs
	}
	return nil
}

func (m *Mirror) serveRpc() {
	server := rpc.NewServer()
	server.Register(m)
	done := make(chan int, 2)
	go func() {
		server.ServeConn(m.rpcConn)
		done <- 1
	}()
	go func() {
		m.worker.content.ServeConn(m.contentConn)
		done <- 1
	}()
	<-done
	m.shutdown(true)
	m.worker.DropMirror(m)
}

func (m *Mirror) shutdown(aggressive bool) {
	m.fsMutex.Lock()
	defer m.fsMutex.Unlock()
	m.accepting = false
	if aggressive {
		m.killed = true
	}

	for fs := range m.activeFses {
		if len(fs.tasks) == 0 {
			delete(m.activeFses, fs)
		}
	}

	if aggressive {
		m.rpcFs.Close()
		for fs := range m.activeFses {
			for t := range fs.tasks {
				t.Kill()
			}
		}
	}
	for len(m.activeFses) > 0 {
		m.cond.Wait()
	}

	m.rpcConn.Close()
	m.contentConn.Close()
	m.fuseFS.Stop()
}

func (m *Mirror) runningCount() int {
	r := 0
	for fs := range m.activeFses {
		r += len(fs.tasks)
	}
	return r
}

var ShuttingDownError error

func init() {
	ShuttingDownError = fmt.Errorf("shutting down")
}

func (m *Mirror) newFs(t *WorkerTask) (fs *workerFSState, err error) {
	m.fsMutex.Lock()
	defer m.fsMutex.Unlock()

	m.waiting++
	for m.runningCount() >= m.maxJobCount {
		m.cond.Wait()
	}
	m.waiting--

	if !m.accepting {
		return nil, ShuttingDownError
	}

	for fs := range m.activeFses {
		if !fs.reaping && len(fs.taskIds) < m.worker.options.ReapCount {
			fs.addTask(t)
			return fs, nil
		}
	}

	wfs, err := m.fuseFS.addWorkerFS()
	if err != nil {
		return nil, err
	}

	m.prepareFS(wfs.state)
	wfs.state.addTask(t)
	m.activeFses[wfs.state] = true
	return wfs.state, nil
}

// Must hold lock.
func (m *Mirror) prepareFS(fs *workerFSState) {
	fs.reaping = false
	fs.taskIds = make([]int, 0, m.worker.options.ReapCount)
}

func (m *Mirror) considerReap(fs *workerFSState, task *WorkerTask) bool {
	m.fsMutex.Lock()
	defer m.fsMutex.Unlock()
	delete(fs.tasks, task)
	if len(fs.tasks) == 0 {
		fs.reaping = true
	}
	m.cond.Broadcast()
	return fs.reaping
}

func (m *Mirror) reapFuse(state *workerFSState) (results *attr.FileSet, taskIds []int, reads []string) {
	log.Printf("Reaping fuse FS %v", state.fs.id)

	ids := state.taskIds[:]
	results, reads = m.fillReply(state)

	return results, ids, reads
}

func (m *Mirror) returnFS(state *workerFSState) {
	m.fsMutex.Lock()
	defer m.fsMutex.Unlock()

	if state.reaping {
		m.prepareFS(state)
	}

	state.fs.SetDebug(false)
	if !m.accepting {
		delete(m.activeFses, state)
		m.cond.Broadcast()
	}
}

func (m *Mirror) Update(req *UpdateRequest, rep *UpdateResponse) error {
	m.updateFiles(req.Files)
	return nil
}

func (m *Mirror) updateFiles(attrs []*attr.FileAttr) {
	m.rpcFs.updateFiles(attrs)

	m.fsMutex.Lock()
	defer m.fsMutex.Unlock()

	for state := range m.activeFses {
		state.fs.update(attrs)
	}
}

func (m *Mirror) Run(req *WorkRequest, rep *WorkResponse) error {
	m.worker.stats.Enter("run")

	// Don't run m.updateFiles() as we don't want to issue
	// unneeded cache invalidations.
	task, err := m.newWorkerTask(req, rep)
	if err != nil {
		return err
	}

	err = task.Run()
	if err != nil {
		log.Println("task.Run:", err)
		return err
	}

	log.Println(rep)
	rep.WorkerId = fmt.Sprintf("%s: %s", Hostname, m.worker.listener.Addr().String())
	m.worker.stats.Exit("run")

	if m.killed {
		return fmt.Errorf("killed worker %s", m.worker.listener.Addr().String())
	}
	return nil
}

const _DELETIONS = "DELETIONS"

func (m *Mirror) newWorkerTask(req *WorkRequest, rep *WorkResponse) (*WorkerTask, error) {
	var stdin io.ReadWriteCloser
	if req.StdinId != "" {
		stdin = m.worker.listener.Pending().accept(req.StdinId)
	}
	task := &WorkerTask{
		req:       req,
		rep:       rep,
		stdinConn: stdin,
		mirror:    m,
		taskInfo:  fmt.Sprintf("%v, dir %v", req.Argv, req.Dir),
	}
	return task, nil
}
