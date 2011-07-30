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

	Waiting              int
	maxJobCount          int
	fuseFileSystemsMutex sync.Mutex
	// TODO - rename to unused FS.
	fuseFileSystems    []*WorkerFuseFs
	workingFileSystems map[*WorkerFuseFs]string
	shuttingDown       bool
	cond               sync.Cond
}

func NewMirror(daemon *WorkerDaemon, rpcConn, revConn net.Conn) *Mirror {
	log.Println("Mirror for", rpcConn, revConn)

	mirror := &Mirror{
		fileServerConn:     revConn,
		rpcConn:            rpcConn,
		fileServer:         rpc.NewClient(revConn),
		daemon:             daemon,
		workingFileSystems: make(map[*WorkerFuseFs]string),
	}
	mirror.rpcFs = NewRpcFs(mirror.fileServer, daemon.contentCache)
	mirror.rpcFs.localRoots = []string{"/lib", "/usr"}
	mirror.cond.L = &mirror.fuseFileSystemsMutex

	go mirror.serveRpc()
	return mirror
}

func (me *Mirror) DiscardFuse(wfs *WorkerFuseFs) {
	wfs.Stop()

	me.fuseFileSystemsMutex.Lock()
	defer me.fuseFileSystemsMutex.Unlock()
	me.workingFileSystems[wfs] = "", false
	me.cond.Signal()
}

func (me *Mirror) serveRpc() {
	server := rpc.NewServer()
	server.Register(me)
	server.ServeConn(me.rpcConn)
	me.Shutdown()
}

func (me *Mirror) Shutdown() {
	me.fuseFileSystemsMutex.Lock()
	defer me.fuseFileSystemsMutex.Unlock()
	if me.shuttingDown {
		return
	}
	me.shuttingDown = true
	me.fileServerConn.Close()
	me.fuseFileSystems = []*WorkerFuseFs{}

	for len(me.workingFileSystems) > 0 {
		me.cond.Wait()
	}
	me.rpcConn.Close()

	go me.daemon.DropMirror(me)
}

func (me *Mirror) getWorkerFuseFs(name string) (f *WorkerFuseFs, err os.Error) {
	me.fuseFileSystemsMutex.Lock()
	defer me.fuseFileSystemsMutex.Unlock()

	me.Waiting++
	for len(me.workingFileSystems) >= me.maxJobCount {
		me.cond.Wait()
	}
	me.Waiting--
	if me.shuttingDown {
		return nil, os.NewError("shutting down")
	}

	l := len(me.fuseFileSystems)
	if l > 0 {
		f = me.fuseFileSystems[l-1]
		me.fuseFileSystems = me.fuseFileSystems[:l-1]
	}
	if f == nil {
		f, err = me.newWorkerFuseFs()
	}
	me.workingFileSystems[f] = name
	return f, err
}

func (me *Mirror) Update(req *UpdateRequest, rep *UpdateResponse) os.Error {
	me.updateFileSystems(req.Files)
	return me.rpcFs.Update(req, rep)
}

func (me *Mirror) updateFileSystems(attrs []AttrResponse) {
	me.fuseFileSystemsMutex.Lock()
	defer me.fuseFileSystemsMutex.Unlock()

	for _, fs := range me.fuseFileSystems {
		fs.update(attrs)
	}
	for fs, _ := range me.workingFileSystems {
		fs.update(attrs)
	}
}

func (me *Mirror) Run(req *WorkRequest, rep *WorkReply) os.Error {
	task, err := me.newWorkerTask(req, rep)
	if err != nil {
		return err
	}

	err = task.Run()
	if err != nil {
		log.Println("Error", err)
		return err
	}

	updateReq := UpdateRequest{
		Files: rep.Files,
	}
	updateRep := UpdateResponse{}
	err = me.Update(&updateReq, &updateRep)
	if err != nil {
		return err
	}

	summary := rep
	// Trim output.

	summary.Stdout = trim(summary.Stdout)
	summary.Stderr = trim(summary.Stderr)

	log.Println("sending back", summary)
	return nil
}

const _DELETIONS = "DELETIONS"

func (me *Mirror) newWorkerFuseFs() (*WorkerFuseFs, os.Error) {
	return newWorkerFuseFs(me.daemon.tmpDir, me.rpcFs, me.writableRoot)
}

func (me *Mirror) newWorkerTask(req *WorkRequest, rep *WorkReply) (*WorkerTask, os.Error) {
	fuseFs, err := me.getWorkerFuseFs(req.Summary())
	if err != nil {
		return nil, err
	}
	var stdin net.Conn
	if req.StdinId != "" {
		stdin = me.daemon.pending.WaitConnection(req.StdinId)
	}
	return &WorkerTask{
		WorkRequest: req,
		WorkReply:   rep,
		stdinConn:   stdin,
		mirror:      me,
		fuseFs:      fuseFs,
	}, nil
}

func (me *Mirror) Status(req *StatusRequest, rep *StatusReply) os.Error {
	me.fuseFileSystemsMutex.Lock()
	defer me.fuseFileSystemsMutex.Unlock()

	rep.Processes += len(me.workingFileSystems)
	return nil
}

func (me *Mirror) FileContent(req *ContentRequest, rep *ContentResponse) os.Error {
	return me.daemon.contentServer.FileContent(req, rep)
}
