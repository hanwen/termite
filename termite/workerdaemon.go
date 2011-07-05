package termite

import (
	"fmt"
	"log"
	"os"
	"rpc"
	"sync"
	"net"
)

var _ = log.Println

type FileInfo struct {
	Delete      bool
	Path        string
	LinkContent string
	os.FileInfo
	Hash []byte
}

// TODO - WorkReply/WorkRequest is cute and simple, but doesn't work
// for Makefiles that use redirection, like the Makefiles of the Linux
// kernel. We can't read stdin on the master, because we don't know in advance if
// it is needed, and detecting it on the worker requires some sort
// streaming RPC.  An alternative is to use multiplexing of the
// connection carrying the RPC to hook up the worker's
// stdin/stdout/stderr directly with the invoking tool.
//
// For now, we can't compile the linux kernel.
type WorkReply struct {
	Exit   *os.Waitmsg
	Files  []FileInfo
	Stderr string
	Stdout string
}

type WorkRequest struct {
	Id         string
	FileServer string
	WritableRoot string
	Binary     string
	Argv       []string
	Env        []string
	Dir        string
}

// State associated with one master.
type MasterWorker struct {
	daemon *WorkerDaemon
	fileServer *rpc.Client
	readonlyRpcFs  *RpcFs
	writableRoot string

	fuseFileSystemsMutex sync.Mutex
	fuseFileSystems []*WorkerFuseFs
}

func (me *MasterWorker) ReturnFuse(wfs *WorkerFuseFs) {
	wfs.unionFs.DropBranchCache()
	wfs.unionFs.DropDeletionCache()

	me.fuseFileSystemsMutex.Lock()
	defer me.fuseFileSystemsMutex.Unlock()
	me.fuseFileSystems = append(me.fuseFileSystems, wfs)
}

func (me *MasterWorker) getWorkerFuseFs() (f *WorkerFuseFs, err os.Error) {
	me.fuseFileSystemsMutex.Lock()
	l := len(me.fuseFileSystems)
	if l > 0 {
		f = me.fuseFileSystems[l-1]
		me.fuseFileSystems = me.fuseFileSystems[:l-1]
	}
	me.fuseFileSystemsMutex.Unlock()
	if f == nil {
		f, err = me.newWorkerFuseFs()
	}
	return 	f, err
}

func (me *WorkerDaemon) newMasterWorker(addr string, writableRoot string) (*MasterWorker, os.Error) {
	conn, err := SetupClient(addr, me.secret)
	if err != nil {
		return nil, err
	}

	w := &MasterWorker{
		fileServer: rpc.NewClient(conn),
		daemon: me,
		writableRoot: writableRoot,
	}
	w.readonlyRpcFs = NewRpcFs(w.fileServer, me.contentCache)

	return w, nil
}

func (me *MasterWorker) Run(req *WorkRequest, rep *WorkReply) os.Error {
	task, err := me.newWorkerTask(req, rep)

	err = task.Run()
	if err != nil {
		log.Println("Error", err)
		return err
	}

	summary := rep
	// Trim output.

	summary.Stdout = trim(summary.Stdout)
	summary.Stderr = trim(summary.Stderr)

	log.Println("sending back", summary)
	return nil
}

type WorkerDaemon struct {
	secret             []byte
	ChrootBinary       string

	// TODO - deal with closed connections.
	masterMapMutex sync.Mutex
	masterMap     map[string]*MasterWorker
	contentCache  *DiskFileCache
	contentServer *ContentServer

	pending   *PendingConnections
}

func (me *WorkerDaemon) getMasterWorker(addr string, writableRoot string) (*MasterWorker, os.Error) {
	me.masterMapMutex.Lock()
	defer me.masterMapMutex.Unlock()

	key := fmt.Sprintf("%s:%s", addr, writableRoot)
	mw, ok := me.masterMap[key]
	if ok {
		return mw, nil
	}

	mw, err := me.newMasterWorker(addr, writableRoot)
	if err != nil {
		return nil, err
	}
	log.Println("Created new MasterWorker for", key)
	me.masterMap[key] = mw
	return mw, err
}

func NewWorkerDaemon(secret []byte, cacheDir string) *WorkerDaemon {
	cache := NewDiskFileCache(cacheDir)
	w := &WorkerDaemon{
		secret:        secret,
		contentCache:  cache,
		masterMap:     make(map[string]*MasterWorker),
		contentServer: &ContentServer{Cache: cache},
		connections:   NewPendingConnections(),
	}
	return w
}

// TODO - should expose under ContentServer name?
func (me *WorkerDaemon) FileContent(req *ContentRequest, rep *ContentResponse) os.Error {
	return me.contentServer.FileContent(req, rep)
}

func trim(s string) string {
	l := 1024
	if len(s) < l {
		l = len(s)
	}
	return s[:l]
}

func (me *WorkerDaemon) Run(req *WorkRequest, rep *WorkReply) os.Error {
	wm, err := me.getMasterWorker(req.FileServer, req.WritableRoot)
	if err != nil {
		return err
	}

	return wm.Run(req, rep)
}

func (me *WorkerDaemon) RunWorkerServer(port int) {
	go me.listen(port)

	fmt.Println("RunWorkerServer")
	rpcServer := rpc.NewServer()
	rpcServer.Register(me)
	conn := me.pending.WaitConnection(RPC_CHANNEL)
	rpcServer.ServeConn(conn)
}

func (me *WorkerDaemon) listen(port int) {
	out := make(chan net.Conn)
	go SetupServer(port, me.secret, out)
	for {
		conn := <-out
		log.Println("connection from", conn.RemoteAddr())
		me.pending.Accept(conn)
	}
}

