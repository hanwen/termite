package termite

import (
	"net"
	"os"
	"log"
	"rpc"
	"sync"
)

// State associated with one master.
type Mirror struct {
	daemon         *WorkerDaemon
	fileServer     *rpc.Client
	fileServerConn net.Conn
	rpcConn        net.Conn
	rpcFs          *RpcFs
	writableRoot   string

	// key in WorkerDaemon's map.
	key string

	maxJobCount int

	fsMutex      sync.Mutex
	cond         *sync.Cond
	waiting      int
	nextId       int
	activeFses   map[*workerFuseFs]bool
	shuttingDown bool
}

func NewMirror(daemon *WorkerDaemon, rpcConn, revConn net.Conn) *Mirror {
	log.Println("Mirror for", rpcConn, revConn)

	mirror := &Mirror{
		activeFses:     map[*workerFuseFs]bool{},
		fileServerConn: revConn,
		rpcConn:        rpcConn,
		fileServer:     rpc.NewClient(revConn),
		daemon:         daemon,
	}
	mirror.cond = sync.NewCond(&mirror.fsMutex)
	mirror.rpcFs = NewRpcFs(mirror.fileServer, daemon.contentCache)
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
	me.fileServer.Close()
	me.fileServerConn.Close()
	for fs := range me.activeFses {
		if len(fs.tasks) == 0 {
			fs.Stop()
			me.activeFses[fs] = false, false
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

func (me *Mirror) newFs(t *WorkerTask) (fs *workerFuseFs, err os.Error) {
	me.fsMutex.Lock()
	defer me.fsMutex.Unlock()

	me.waiting++
	for !me.shuttingDown && me.runningCount() >= me.maxJobCount {
		me.cond.Wait()
	}
	me.waiting--

	if me.shuttingDown {
		return nil, os.NewError("shutting down")
	}

	for fs := range me.activeFses {
		if !fs.reaping && fs.reapCountdown > 0 {
			fs.reapCountdown--
			fs.tasks[t] = true
			return fs, nil
		}
	}
	fs, err = me.newWorkerFuseFs()
	if err != nil {
		return nil, err
	}

	me.prepareFs(fs)
	fs.tasks[t] = true
	me.activeFses[fs] = true
	return fs, nil
}

// Must hold lock.
func (me *Mirror) prepareFs(fs *workerFuseFs) {
	fs.reaping = false
	fs.id = me.nextId
	me.nextId++
	fs.reapCountdown = me.daemon.options.ReapCount
}

func (me *Mirror) considerReap(fs *workerFuseFs, task *WorkerTask) bool {
	me.fsMutex.Lock()
	defer me.fsMutex.Unlock()
	fs.tasks[task] = false, false
	if len(fs.tasks) == 0 {
		fs.reaping = true
	}
	me.cond.Broadcast()
	return fs.reaping
}

func (me *Mirror) reapFuse(fs *workerFuseFs) (results *FileSet) {
	log.Printf("Reaping fuse FS %d", fs.id)
	results = me.fillReply(fs.unionFs)

	// Must do updateFiles before ReturnFuse, since the
	// next job should not see out-of-date files.
	me.updateFiles(results.Files, fs)
	return results
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
		me.activeFses[fs] = false, false
		me.cond.Broadcast()
	}
}

func (me *Mirror) Update(req *UpdateRequest, rep *UpdateResponse) os.Error {
	me.updateFiles(req.Files, nil)
	return nil
}

func (me *Mirror) updateFiles(attrs []*FileAttr, origin *workerFuseFs) {
	me.rpcFs.updateFiles(attrs)

	me.fsMutex.Lock()
	defer me.fsMutex.Unlock()

	for fs := range me.activeFses {
		fs.update(attrs, origin)
	}
}

func (me *Mirror) Run(req *WorkRequest, rep *WorkResponse) os.Error {
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
	return nil
}

const _DELETIONS = "DELETIONS"

func (me *Mirror) newWorkerFuseFs() (*workerFuseFs, os.Error) {
	return newWorkerFuseFs(me.daemon.tmpDir, me.rpcFs, me.writableRoot, me.daemon.Nobody)
}

func (me *Mirror) newWorkerTask(req *WorkRequest, rep *WorkResponse) (*WorkerTask, os.Error) {
	var stdin net.Conn
	if req.StdinId != "" {
		stdin = me.daemon.pending.WaitConnection(req.StdinId)
	}
	task := &WorkerTask{
		WorkRequest:  req,
		WorkResponse: rep,
		stdinConn:    stdin,
		mirror:       me,
	}
	return task, nil
}

func (me *Mirror) FileContent(req *ContentRequest, rep *ContentResponse) os.Error {
	return me.daemon.contentServer.FileContent(req, rep)
}
