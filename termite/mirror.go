package termite

import (
	"fmt"
	"path/filepath"
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
	daemon        *WorkerDaemon
	fileServer    *rpc.Client
	rpcFs         *RpcFs
	writableRoot  string

	fuseFileSystemsMutex sync.Mutex
	fuseFileSystems      []*WorkerFuseFs
	workingFileSystems   map[*WorkerFuseFs]string
}

func (me *Mirror) ReturnFuse(wfs *WorkerFuseFs) {
	wfs.unionFs.DropBranchCache()
	wfs.unionFs.DropDeletionCache()

	me.fuseFileSystemsMutex.Lock()
	defer me.fuseFileSystemsMutex.Unlock()
	me.fuseFileSystems = append(me.fuseFileSystems, wfs)
	me.workingFileSystems[wfs] = "", false
}

func (me *Mirror) getWorkerFuseFs(name string) (f *WorkerFuseFs, err os.Error) {
	me.fuseFileSystemsMutex.Lock()
	defer me.fuseFileSystemsMutex.Unlock()
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
	w.MountState.Mount(w.mount, &fuse.MountOptions{AllowOther: true})
	if err != nil {
		return nil, err
	}

	go w.MountState.Loop(true)

	return &w, nil
}

func (me *Mirror) newWorkerTask(req *WorkRequest, rep *WorkReply) (*WorkerTask, os.Error) {
	fuseFs, err := me.getWorkerFuseFs(fmt.Sprintf("%v", req))
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
	}, nil
}
