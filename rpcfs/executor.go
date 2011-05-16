package rpcfs

import (
	"os"
//	"path/filepath"
	"log"
//	"sync"
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

	stdout *os.File
	stderr *os.File
	stdin *os.File

	*fuse.MountState
}

func (me *WorkerTask) Run() os.Error {
	attr := os.ProcAttr{
	Dir: me.Task.Dir,
	Env: me.Task.Env,
        Files: []*os.File{me.stdin, me.stdout, me.stderr},
	}

	bin := "/usr/sbin/chroot"
	cmd := []string{bin, me.mount, "--userspec=nobody:nobody"}

	newcmd := make([]string, len(cmd) + len(me.Task.Argv))
	copy(newcmd, cmd)
	copy(newcmd[len(cmd):], me.Task.Argv)
	
	proc, err := os.StartProcess(bin, me.Task.Argv, &attr)
	if err != nil {
		return err
	}

	msg, err := proc.Wait(0)
	log.Println("result:", msg)
	return err		
}

func NewWorkerTask(server *rpc.Client, task *Task) (*WorkerTask, os.Error) {
	w := &WorkerTask{}
	
	tmpDir, err := ioutil.TempDir("rpcfs", "")
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

	w.stderr, err = os.OpenFile(w.tmpDir + "/stdout", os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}
	w.stdout, err = os.OpenFile(w.tmpDir + "/stderr", os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}
	f, err := os.Create(w.tmpDir + "/stdin")
	if err != nil {
		return nil, err
	}
	f.Close()
	w.stdin, err = os.Open(w.tmpDir + "/stdin")
	if err != nil {
		return nil, err
	}
	
	fs := fuse.NewLoopbackFileSystem(w.rwDir)
	roFs := NewRpcFs(server)

	ttl := 100.0
	opts := unionfs.UnionFsOptions{
		BranchCacheTTLSecs: ttl,
		DeletionCacheTTLSecs:ttl,
		DeletionDirName: "DELETIONS",
	}
	mOpts := fuse.MountOptions{
		EntryTimeout: ttl,
		AttrTimeout: ttl,
		NegativeTimeout: ttl,
	}
	
	ufs := unionfs.NewUnionFs("ufs", []fuse.FileSystem{fs, roFs}, opts)
	state, _, err := fuse.MountFileSystem(w.mount, ufs, &mOpts)
	if err != nil {
		return nil, err
	}
	
	w.MountState = state
	return w, nil
}

