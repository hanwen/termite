package rpcfs

import (
	"os"
	"log"
	"io/ioutil"
	"rpc"
	"github.com/hanwen/go-fuse/fuse"
	"github.com/hanwen/go-fuse/unionfs"
	)

type Task struct {
	Argv []string
	Env []string
	Dir string
}

type WorkerTask struct {
	fileServer *rpc.Client
	mount string
	rwDir string
	tmpDir string
	*Task

	*fuse.MountState
}
func (me *WorkerTask) Stop() {
	log.Println("unmounting..")
	me.MountState.Unmount()
}

func (me *WorkerTask) Run() os.Error {
	defer me.Stop()

	rStdout, wStdout, err := os.Pipe()
	rStderr, wStderr, err := os.Pipe()
	
	attr := os.ProcAttr{
	Dir: me.Task.Dir,
	Env: me.Task.Env,
        Files: []*os.File{nil, wStdout, wStderr},
	}

	// This is a security hole, but convenient for testing.
	bin := "/tmp/chroot-suid"
	cmd := []string{bin, me.mount}

	newcmd := make([]string, len(cmd) + len(me.Task.Argv))
	copy(newcmd, cmd)
	copy(newcmd[len(cmd):], me.Task.Argv)

	log.Println("starting cmd", newcmd)
	proc, err := os.StartProcess(bin, newcmd, &attr)
	if err != nil {
		log.Println("Error", err)
		return err
	}

	wStdout.Close()
	wStderr.Close()
	
	stdout, err := ioutil.ReadAll(rStdout)
	stderr, err := ioutil.ReadAll(rStderr)
	
	msg, err := proc.Wait(0)
	
	log.Println("stdout:", string(stdout))
	log.Println("stderr:", string(stderr))
	log.Println("result:", msg, "dir:", me.tmpDir)
	return err		
}

func NewWorkerTask(server *rpc.Client, task *Task) (*WorkerTask, os.Error) {
	w := &WorkerTask{}
	
	tmpDir, err := ioutil.TempDir("", "rpcfs-tmp")
	w.tmpDir = tmpDir
	if err != nil {
		return nil, err
	}
	w.rwDir = w.tmpDir + "/rw"
	err = os.Mkdir(w.rwDir, 0700)
	if err != nil {
		return nil, err
	}
	w.mount = w.tmpDir + "/mnt"
	err = os.Mkdir(w.mount, 0700)
	if err != nil {
		return nil, err
	}
	w.Task = task

	fs := fuse.NewLoopbackFileSystem(w.rwDir)
	roFs := NewRpcFs(server)

	// High ttl, since all writes come through fuse.
	ttl := 100.0
	opts := unionfs.UnionFsOptions{
		BranchCacheTTLSecs: ttl,
		DeletionCacheTTLSecs:ttl,
		DeletionDirName: "DELETIONS",
	}
	mOpts := fuse.FileSystemOptions{
		EntryTimeout: ttl,
		AttrTimeout: ttl,
		NegativeTimeout: ttl,
	}
	
	ufs := unionfs.NewUnionFs("ufs", []fuse.FileSystem{fs, roFs}, opts)
	conn := fuse.NewFileSystemConnector(ufs, &mOpts)
	state := fuse.NewMountState(conn)
	state.Mount(w.mount, &fuse.MountOptions{AllowOther: true})
	if err != nil {
		return nil, err
	}
	
	w.MountState = state
	go state.Loop(true)
	return w, nil
}

