package rpcfs

import (
	"fmt"
	"path/filepath"
	"os"
	"log"
	"io/ioutil"
	"github.com/hanwen/go-fuse/fuse"
	"github.com/hanwen/go-fuse/unionfs"
	"os/user"
	"strings"
	"syscall"
)

type WorkerTask struct {
	mount  string
	rwDir  string
	tmpDir string
	*WorkRequest
	*WorkReply
	daemon *WorkerDaemon

	*fuse.MountState
}

const _DELETIONS = "DELETIONS"

func (me *WorkerTask) Stop() {
	log.Println("unmounting..")
	me.MountState.Unmount()
	os.RemoveAll(me.tmpDir)
}

func (me *WorkerTask) RWDir() string {
	return me.rwDir
}

func (me *WorkerDaemon) newWorkerTask(req *WorkRequest, rep *WorkReply) (*WorkerTask, os.Error) {
	fs, err := me.getFileServer(req.FileServer)
	if err != nil {
		return nil, err
	}

	w := &WorkerTask{
		WorkRequest: req,
		WorkReply:   rep,
		daemon:      me,
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

	rwFs := fuse.NewLoopbackFileSystem(w.rwDir)
	roFs := NewRpcFs(fs, me.contentCache)

	// High ttl, since all writes come through fuse.
	ttl := 100.0
	opts := unionfs.UnionFsOptions{
		BranchCacheTTLSecs:   ttl,
		DeletionCacheTTLSecs: ttl,
		DeletionDirName:      _DELETIONS,
	}
	mOpts := fuse.FileSystemOptions{
		EntryTimeout:    ttl,
		AttrTimeout:     ttl,
		NegativeTimeout: ttl,
	}

	ufs := unionfs.NewUnionFs("ufs", []fuse.FileSystem{rwFs, roFs}, opts)
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

func (me *WorkerTask) Run() os.Error {
	defer me.Stop()

	rStdout, wStdout, err := os.Pipe()
	rStderr, wStderr, err := os.Pipe()

	attr := os.ProcAttr{
		Env:   me.WorkRequest.Env,
		Files: []*os.File{nil, wStdout, wStderr},
	}

	nobody, err := user.Lookup("nobody")
	if err != nil {
		return err
	}

	chroot := me.daemon.ChrootBinary
	cmd := []string{chroot, "-dir", me.WorkRequest.Dir,
		"-uid", fmt.Sprintf("%d", nobody.Uid), "-gid", fmt.Sprintf("%d", nobody.Gid),
		"-binary", me.WorkRequest.Binary,
		me.mount}

	newcmd := make([]string, len(cmd)+len(me.WorkRequest.Argv))
	copy(newcmd, cmd)
	copy(newcmd[len(cmd):], me.WorkRequest.Argv)

	log.Println("starting cmd", newcmd)
	proc, err := os.StartProcess(chroot, newcmd, &attr)
	if err != nil {
		log.Println("Error", err)
		return err
	}

	wStdout.Close()
	wStderr.Close()

	stdout, err := ioutil.ReadAll(rStdout)
	stderr, err := ioutil.ReadAll(rStderr)

	me.WorkReply.Exit, err = proc.Wait(0)
	me.WorkReply.Stdout = string(stdout)
	me.WorkReply.Stderr = string(stderr)

	// TODO - look at rw directory, and serialize the files into WorkReply.
	err = me.fillReply()
	return err
}

func (me *WorkerTask) VisitFile(path string, osInfo *os.FileInfo) {
	fi := FileInfo{
		FileInfo: *osInfo,
	}

	ftype := osInfo.Mode &^ 07777
	switch ftype {
	case syscall.S_IFREG:
		fi.Hash = me.daemon.contentCache.SavePath(path)
	case syscall.S_IFLNK:
		val, err := os.Readlink(path)
		if err != nil {
			// TODO - fail rpc.
			log.Fatal("Readlink error.")
		}
		fi.LinkContent = val
	default:
		log.Fatalf("Unknown file type %o", ftype)
	}

	me.savePath(path, fi)
}

func (me *WorkerTask) savePath(path string, fi FileInfo) {
	if !strings.HasPrefix(path, me.rwDir) {
		log.Println("Weird file", path)
		return
	}
	fi.Path = path[len(me.rwDir):]
	me.WorkReply.Files = append(me.WorkReply.Files, fi)
}

func (me *WorkerTask) VisitDir(path string, osInfo *os.FileInfo) bool {
	me.savePath(path, FileInfo{FileInfo: *osInfo})
	return true
}

func (me *WorkerTask) fillReply() os.Error {
	dir := filepath.Join(me.rwDir, _DELETIONS)
	_, err := os.Lstat(dir)
	if err == nil {
		matches, err := filepath.Glob(dir + "/*")
		if err != nil {
			return err
		}

		for _, m := range matches {
			contents, err := ioutil.ReadFile(filepath.Join(dir, m))
			if err != nil {
				return err
			}

			me.WorkReply.Files = append(me.WorkReply.Files, FileInfo{
				Delete: true,
				Path:   string(contents),
			})
		}

		err = os.RemoveAll(dir)
		if err != nil {
			return err
		}
	}
	filepath.Walk(me.rwDir, me, nil)
	return nil
}
