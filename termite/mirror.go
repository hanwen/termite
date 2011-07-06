package termite

import (
	"path/filepath"
	"net"
	"os"
	"log"
	"io/ioutil"
	"github.com/hanwen/go-fuse/fuse"
	"github.com/hanwen/go-fuse/unionfs"
	"rpc"
	"sync"
)

// State associated with one master.
type Mirror struct {
	daemon       *WorkerDaemon
	fileServer   *rpc.Client
	fileServerConn net.Conn
	rpcConn      net.Conn
	rpcFs        *RpcFs
	writableRoot string

	// key in WorkerDaemon's map.
	key          string

	Waiting     int
	maxJobCount int
	fuseFileSystemsMutex sync.Mutex
	fuseFileSystems      []*WorkerFuseFs
	workingFileSystems   map[*WorkerFuseFs]string
	shuttingDown         bool
	cond sync.Cond
}

func NewMirror(daemon *WorkerDaemon, rpcConn, revConn net.Conn) *Mirror {
	log.Println("Mirror for", rpcConn, revConn)

	mirror := &Mirror{
		fileServerConn: revConn,
		rpcConn: 	rpcConn,
		fileServer:         rpc.NewClient(revConn),
		daemon:             daemon,
		workingFileSystems: make(map[*WorkerFuseFs]string),
	}
	mirror.rpcFs = NewRpcFs(mirror.fileServer, daemon.contentCache)
	mirror.cond.L = &mirror.fuseFileSystemsMutex

	go mirror.serveRpc()
	return mirror
}

func (me *Mirror) ReturnFuse(wfs *WorkerFuseFs) {
	// TODO - could be more fine-grained here.
	wfs.unionFs.DropBranchCache()
	wfs.unionFs.DropDeletionCache()

	me.fuseFileSystemsMutex.Lock()
	defer me.fuseFileSystemsMutex.Unlock()

	if !me.shuttingDown {
		wfs.Stop()
	} else {
		me.fuseFileSystems = append(me.fuseFileSystems, wfs)
	}
	me.workingFileSystems[wfs] = "", false
	me.cond.Signal()
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
	log.Println("getWorkerFuseFs", name)
	
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
	return me.rpcFs.Update(req, rep)
}

func (me *Mirror) Run(req *WorkRequest, rep *WorkReply) os.Error {
	task, err := me.newWorkerTask(req, rep)

	err = task.Run()
	if err != nil {
		log.Println("Error", err)
		return err
	}

	updateReq := UpdateRequest{
		Files: rep.Files,
	}
	updateRep := UpdateResponse{}
	err = me.rpcFs.Update(&updateReq, &updateRep)
	if err != nil {
		// TODO - fatal?
		log.Println("Update failed.")
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
	w := WorkerFuseFs{}

	tmpDir, err := ioutil.TempDir("", "rpcfs-tmp")
	w.tmpDir = tmpDir
	type dirInit struct {
		dst *string
		val string
	}

	for _, v := range []dirInit{
		dirInit{&w.rwDir, "rw"},
		dirInit{&w.mount, "mnt"},
	} {
		*v.dst = filepath.Join(w.tmpDir, v.val)
		err = os.Mkdir(*v.dst, 0700)
		if err != nil {
			return nil, err
		}
	}

	rwFs := fuse.NewLoopbackFileSystem(w.rwDir)

	ttl := 5.0
	opts := unionfs.UnionFsOptions{
		BranchCacheTTLSecs:   ttl,
		DeletionCacheTTLSecs: ttl,
		DeletionDirName:      _DELETIONS,
	}
	mOpts := fuse.FileSystemOptions{
		EntryTimeout:    ttl,
		AttrTimeout:     ttl,
		NegativeTimeout: 0.01,
	}

	w.unionFs = unionfs.NewUnionFs("ufs", []fuse.FileSystem{rwFs, me.rpcFs}, opts)
	swFs := []fuse.SwitchedFileSystem{
		{"dev", &DevNullFs{}, true},
		{"", me.rpcFs, false},

		// TODO - configurable.
		{"tmp", w.unionFs, false},
		{me.writableRoot, w.unionFs, false},
	}

	conn := fuse.NewFileSystemConnector(fuse.NewSwitchFileSystem(swFs), &mOpts)
	w.MountState = fuse.NewMountState(conn)

	fuseOpts := fuse.MountOptions{
		AllowOther: true,
		// Compilers are not that highly parallel.  A lower
		// number also helps stacktrace be less overwhelming.
		MaxBackground: 4,
	}
	w.MountState.Mount(w.mount, &fuseOpts)
	if err != nil {
		return nil, err
	}

	go w.MountState.Loop(true)

	return &w, nil
}

func (me *Mirror) newWorkerTask(req *WorkRequest, rep *WorkReply) (*WorkerTask, os.Error) {
	fuseFs, err := me.getWorkerFuseFs(req.String())
	if err != nil {
		return nil, err
	}
	stdin := me.daemon.pending.WaitConnection(req.StdinId)
	return &WorkerTask{
		WorkRequest:  req,
		WorkReply:    rep,
		stdinConn:    stdin,
		masterWorker: me,
		fuseFs:       fuseFs,
	},nil
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
