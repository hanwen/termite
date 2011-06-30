package rpcfs

import (
	"fmt"
	"path/filepath"
	"os"
	"log"
	"io/ioutil"
	"rpc"
	"github.com/hanwen/go-fuse/fuse"
	"github.com/hanwen/go-fuse/unionfs"
	"os/user"
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
	cacheDir string
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
	Env: me.Task.Env,
        Files: []*os.File{nil, wStdout, wStderr},
	}

	nobody, err := user.Lookup("nobody")
	if err != nil {
		return err
	}

	// TODO - configurable.
	bin := "termite/chroot/chroot"
	cmd := []string{bin, "-dir", me.Task.Dir,
		"-uid", fmt.Sprintf("%d", nobody.Uid), "-gid", fmt.Sprintf("%d", nobody.Gid),
		me.mount}

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


func NewWorkerTask(server *rpc.Client, task *Task, cacheDir string) (*WorkerTask, os.Error) {
	w := &WorkerTask{
	cacheDir: cacheDir,
	}
	
	tmpDir, err := ioutil.TempDir("", "rpcfs-tmp")
	type dirInit struct {
		dst *string
		val string
	}
	
	for _, v := range []dirInit{
		dirInit{&w.rwDir, "rw"},
		dirInit{&w.mount, "mnt"},
	} {
		*v.dst = filepath.Join(tmpDir, v.val)
		err = os.Mkdir(*v.dst, 0700)
		if err != nil {
			return nil, err
		}
	}

	w.Task = task

	fs := fuse.NewLoopbackFileSystem(w.rwDir)
	roFs := NewRpcFs(server, w.cacheDir)

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

