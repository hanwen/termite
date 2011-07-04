package termite

import (
	"log"
	"os"
	"rpc"
	"sync"
)

var _ = log.Println

type FileInfo struct {
	Delete      bool
	Path        string
	LinkContent string
	os.FileInfo
	Hash []byte
}

type WorkReply struct {
	Exit   *os.Waitmsg
	Files  []FileInfo
	Stderr string
	Stdout string
}

type WorkRequest struct {
	FileServer string
	Binary     string
	Argv       []string
	Env        []string
	Dir        string
}

// State associated with one master.
type MasterWorker struct {
	daemon *WorkerDaemon
	fileServer *rpc.Client

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


func (me *WorkerDaemon) NewMasterWorker(addr string) (*MasterWorker, os.Error) {
	conn, err := SetupClient(addr, me.secret)
	if err != nil {
		return nil, err
	}

	return &MasterWorker{
		fileServer: rpc.NewClient(conn),
		daemon: me,
	}, nil
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
}

func (me *WorkerDaemon) getMasterWorker(addr string) (*MasterWorker, os.Error) {
	me.masterMapMutex.Lock()
	defer me.masterMapMutex.Unlock()

	mw, ok := me.masterMap[addr]
	if ok {
		return mw, nil
	}

	mw, err := me.NewMasterWorker(addr)
	if err != nil {
		return nil, err
	}

	me.masterMap[addr] = mw
	return mw, err
}

func NewWorkerDaemon(secret []byte, cacheDir string) *WorkerDaemon {
	cache := NewDiskFileCache(cacheDir)
	w := &WorkerDaemon{
		secret:        secret,
		contentCache:  cache,
		masterMap:     make(map[string]*MasterWorker),
		contentServer: &ContentServer{Cache: cache},
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
	wm, err := me.getMasterWorker(req.FileServer)
	if err != nil {
		return err
	}

	return wm.Run(req, rep)
}

