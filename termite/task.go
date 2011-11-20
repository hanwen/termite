package termite

import (
	"bytes"
	"fmt"
	"github.com/hanwen/termite/attr"
	"github.com/hanwen/termite/fs"
	"log"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
)

type WorkerTask struct {
	*WorkRequest
	*WorkResponse
	stdinConn net.Conn
	mirror    *Mirror
	cmd       *exec.Cmd
	taskInfo  string
}

func (me *WorkerTask) Kill() {
	// TODO - racy.
	if me.cmd.Process != nil {
		pid := me.cmd.Process.Pid
		err := syscall.Kill(pid, syscall.SIGQUIT)
		log.Printf("Killed pid %d, result %v", pid, err)
	}
}

func (me *WorkerTask) String() string {
	return me.taskInfo
}

func (me *WorkerTask) Run() error {
	fuseFs, err := me.mirror.newFs(me)
	if err != nil {
		return err
	}

	me.mirror.daemon.stats.Enter("fuse")
	err = me.runInFuse(fuseFs)
	me.mirror.daemon.stats.Exit("fuse")

	defer me.mirror.returnFs(fuseFs)

	me.mirror.daemon.stats.Enter("reap")
	if me.mirror.considerReap(fuseFs, me) {
		me.WorkResponse.FileSet, me.WorkResponse.TaskIds = me.mirror.reapFuse(fuseFs)
	}
	me.mirror.daemon.stats.Exit("reap")

	return err
}

func (me *WorkerTask) runInFuse(fuseFs *workerFuseFs) error {
	fuseFs.SetDebug(me.WorkRequest.Debug)
	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}

	// See /bin/true for the background of
	// /bin/true. http://code.google.com/p/go/issues/detail?id=2373
	me.cmd = &exec.Cmd{
		Path: me.WorkRequest.Binary,
		Args: me.WorkRequest.Argv,
	}
	cmd := me.cmd
	if os.Geteuid() == 0 {
		attr := &syscall.SysProcAttr{}
		attr.Credential = &syscall.Credential{
			Uid: uint32(me.mirror.daemon.options.User.Uid),
			Gid: uint32(me.mirror.daemon.options.User.Gid),
		}
		attr.Chroot = fuseFs.mount

		cmd.SysProcAttr = attr
		cmd.Dir = me.WorkRequest.Dir
	} else {
		cmd.Path = filepath.Join(fuseFs.mount, me.WorkRequest.Binary)
		cmd.Dir = filepath.Join(fuseFs.mount, me.WorkRequest.Dir)
	}

	cmd.Env = me.WorkRequest.Env
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	if me.stdinConn != nil {
		cmd.Stdin = me.stdinConn
	}

	if err := cmd.Start(); err != nil {
		return err
	}

	printCmd := fmt.Sprintf("%v", cmd.Args)
	if me.WorkRequest.Debug {
		printCmd = fmt.Sprintf("%v", cmd)
	}
	me.taskInfo = fmt.Sprintf("%v, dir %v, fuse FS %v",
		printCmd, cmd.Dir, fuseFs.id)
	err := cmd.Wait()

	waitMsg, ok := err.(*exec.ExitError)
	if ok {
		me.WorkResponse.Exit = *waitMsg.Waitmsg
		err = nil
	}

	// No waiting: if the process exited, we kill the connection.
	if me.stdinConn != nil {
		me.stdinConn.Close()
	}

	// We could use a connection here too, but this is simpler.
	me.WorkResponse.Stdout = stdout.String()
	me.WorkResponse.Stderr = stderr.String()

	return err
}

// Sorts FileAttr such deletions come reversed before additions.

func (me *Mirror) fillReply(ufs *fs.MemUnionFs) *attr.FileSet {
	yield := ufs.Reap()
	wrRoot := strings.TrimLeft(me.writableRoot, "/")
	cache := me.daemon.contentCache

	files := []*attr.FileAttr{}
	reapedHashes := map[string]string{}
	for path, v := range yield {
		f := &attr.FileAttr{
			Path: filepath.Join(wrRoot, path),
		}

		f.FileInfo = v.FileInfo
		f.Link = v.Link
		if f.FileInfo != nil && f.FileInfo.IsRegular() {
			contentPath := filepath.Join(wrRoot, v.Original)
			if v.Original != "" && v.Original != contentPath {
				fa := me.rpcFs.attr.Get(contentPath)
				if fa.Hash == "" {
					log.Panicf("Contents for %q disappeared.", contentPath)
				}
				f.Hash = fa.Hash
			}
			if v.Backing != "" {
				f.Hash = reapedHashes[v.Backing]
				var err error
				if f.Hash == "" {
					f.Hash, err = cache.DestructiveSavePath(v.Backing)
				}
				if err != nil {
					log.Fatalf("DestructiveSavePath fail %q: %v", v.Backing, err)
				} else {
					reapedHashes[v.Backing] = f.Hash
				}
			}
		}

		files = append(files, f)
	}
	fs := attr.FileSet{Files: files}
	fs.Sort()

	return &fs
}
