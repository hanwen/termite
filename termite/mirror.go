package termite

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"sync"
)

// State associated with one master.
type Mirror struct {
	daemon       *Worker
	rpcConn      net.Conn
	rpcFs        *RpcFs
	writableRoot string

	// key in Worker's map.
	key string

	maxJobCount int

	fsMutex      sync.Mutex
	cond         *sync.Cond
	waiting      int
	nextFsId     int
	activeFses   map[*workerFuseFs]bool
	shuttingDown bool
}

func NewMirror(daemon *Worker, rpcConn, revConn net.Conn) *Mirror {
	log.Println("Mirror for", rpcConn, revConn)

	mirror := &Mirror{
		activeFses: map[*workerFuseFs]bool{},
		rpcConn:    rpcConn,
		daemon:     daemon,
	}
	mirror.cond = sync.NewCond(&mirror.fsMutex)
	mirror.rpcFs = NewRpcFs(rpc.NewClient(revConn), daemon.contentCache)

	_, portString, _ := net.SplitHostPort(daemon.listener.Addr().String())

	mirror.rpcFs.id = Hostname + ":" + portString
	mirror.rpcFs.attr.Paranoia = daemon.options.Paranoia
	mirror.rpcFs.localRoots = []string{"/lib", "/usr"}

	go mirror.serveRpc()
	return mirror
}

func (me *Mirror) serveRpc() {
	server := rpc.NewServer()
	server.Register(me)
	server.ServeConn(me.rpcConn)
	me.Shutdown()
}

func (me *Mirror) Shutdown() {
	me.fsMutex.Lock()
	defer me.fsMutex.Unlock()
	if me.shuttingDown {
		return
	}
	me.shuttingDown = true
	me.rpcFs.Close()
	for fs := range me.activeFses {
		for t := range fs.tasks {
			t.Kill()
		}
		if len(fs.tasks) == 0 {
			fs.Stop()
			delete(me.activeFses, fs)
		}
	}

	for len(me.activeFses) > 0 {
		me.cond.Wait()
	}
	me.rpcConn.Close()

	go me.daemon.DropMirror(me)
}

func (me *Mirror) runningCount() int {
	r := 0
	for fs := range me.activeFses {
		r += len(fs.tasks)
	}
	return r
}

func (me *Mirror) newFs(t *WorkerTask) (fs *workerFuseFs, err error) {
	me.fsMutex.Lock()
	defer me.fsMutex.Unlock()

	me.waiting++
	for !me.shuttingDown && me.runningCount() >= me.maxJobCount {
		me.cond.Wait()
	}
	me.waiting--

	if me.shuttingDown {
		return nil, errors.New("shutting down")
	}

	for fs := range me.activeFses {
		if !fs.reaping && len(fs.taskIds) < me.daemon.options.ReapCount {
			fs.addTask(t)
			return fs, nil
		}
	}
	fs, err = me.newWorkerFuseFs()
	if err != nil {
		return nil, err
	}

	me.prepareFs(fs)
	fs.addTask(t)
	me.activeFses[fs] = true
	return fs, nil
}

// Must hold lock.
func (me *Mirror) prepareFs(fs *workerFuseFs) {
	fs.reaping = false
	fs.taskIds = make([]int, 0, me.daemon.options.ReapCount)
}

func (me *Mirror) considerReap(fs *workerFuseFs, task *WorkerTask) bool {
	me.fsMutex.Lock()
	defer me.fsMutex.Unlock()
	delete(fs.tasks, task)
	if len(fs.tasks) == 0 {
		fs.reaping = true
	}
	me.cond.Broadcast()
	return fs.reaping
}

func (me *Mirror) reapFuse(fs *workerFuseFs) (results *FileSet, taskIds []int) {
	log.Printf("Reaping fuse FS %v", fs.id)
	results = me.fillReply(fs.unionFs)
	fs.unionFs.Reset()

	return results, fs.taskIds[:]
}

func (me *Mirror) returnFs(fs *workerFuseFs) {
	me.fsMutex.Lock()
	defer me.fsMutex.Unlock()

	if fs.reaping {
		me.prepareFs(fs)
	}

	fs.SetDebug(false)
	if me.shuttingDown {
		fs.Stop()
		delete(me.activeFses, fs)
		me.cond.Broadcast()
	}
}

func (me *Mirror) Update(req *UpdateRequest, rep *UpdateResponse) error {
	me.updateFiles(req.Files)
	return nil
}

func (me *Mirror) updateFiles(attrs []*FileAttr) {
	me.rpcFs.updateFiles(attrs)

	me.fsMutex.Lock()
	defer me.fsMutex.Unlock()

	for fs := range me.activeFses {
		fs.update(attrs)
	}
}

func (me *Mirror) Run(req *WorkRequest, rep *WorkResponse) error {
	me.daemon.stats.Enter("run")
	log.Print("Received request", req)
	
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

	rep.LastTime = 0
	log.Println(rep)
	rep.WorkerId = fmt.Sprintf("%s: %s", Hostname, me.daemon.listener.Addr().String())
	me.daemon.stats.Exit("run")
	return nil
}

const _DELETIONS = "DELETIONS"

func (me *Mirror) newWorkerFuseFs() (*workerFuseFs, error) {
	f, err := newWorkerFuseFs(me.daemon.options.TempDir, me.rpcFs, me.writableRoot,
		me.daemon.options.User)

	f.id = fmt.Sprintf("%d", me.nextFsId)
	me.nextFsId++

	return f, err
}

func (me *Mirror) newWorkerTask(req *WorkRequest, rep *WorkResponse) (*WorkerTask, error) {
	var stdin net.Conn
	if req.StdinId != "" {
		stdin = me.daemon.pending.WaitConnection(req.StdinId)
	}
	task := &WorkerTask{
		WorkRequest:  req,
		WorkResponse: rep,
		stdinConn:    stdin,
		mirror:       me,
		taskInfo:     fmt.Sprintf("%v, dir %v", req.Argv, req.Dir),
	}
	return task, nil
}

func (me *Mirror) FileContent(req *ContentRequest, rep *ContentResponse) error {
	return me.daemon.contentCache.Serve(req, rep)
}
