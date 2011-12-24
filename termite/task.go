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
	"time"
)

type WorkerTask struct {
	req       *WorkRequest
	rep       *WorkResponse
	stdinConn net.Conn
	mirror    *Mirror
	cmd       *exec.Cmd
	taskInfo  string
}

func (me *WorkerTask) Kill() {
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

	me.mirror.worker.stats.Enter("fuse")
	err = me.runInFuse(fuseFs)
	me.mirror.worker.stats.Exit("fuse")

	defer me.mirror.returnFs(fuseFs)

	me.mirror.worker.stats.Enter("reap")
	if me.mirror.considerReap(fuseFs, me) {
		me.rep.FileSet, me.rep.TaskIds = me.mirror.reapFuse(fuseFs)
	}
	me.mirror.worker.stats.Exit("reap")

	return err
}

func (me *WorkerTask) runInFuse(fuseFs *workerFuseFs) error {
	fuseFs.SetDebug(me.req.Debug)
	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}

	// See /bin/true for the background of
	// /bin/true. http://code.google.com/p/go/issues/detail?id=2373
	me.cmd = &exec.Cmd{
		Path: me.req.Binary,
		Args: me.req.Argv,
	}
	cmd := me.cmd
	if os.Geteuid() == 0 {
		attr := &syscall.SysProcAttr{}
		attr.Credential = &syscall.Credential{
			Uid: uint32(me.mirror.worker.options.User.Uid),
			Gid: uint32(me.mirror.worker.options.User.Gid),
		}
		attr.Chroot = fuseFs.mount

		cmd.SysProcAttr = attr
		cmd.Dir = me.req.Dir
	} else {
		cmd.Path = filepath.Join(fuseFs.mount, me.req.Binary)
		cmd.Dir = filepath.Join(fuseFs.mount, me.req.Dir)
	}

	cmd.Env = me.req.Env
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	if me.stdinConn != nil {
		cmd.Stdin = me.stdinConn
	}

	if err := cmd.Start(); err != nil {
		return err
	}

	printCmd := fmt.Sprintf("%v", cmd.Args)
	if me.req.Debug {
		printCmd = fmt.Sprintf("%v", cmd)
	}
	me.taskInfo = fmt.Sprintf("%v, dir %v, fuse FS %v",
		printCmd, cmd.Dir, fuseFs.id)
	err := cmd.Wait()

	waitMsg, ok := err.(*exec.ExitError)
	if ok {
		me.rep.Exit = *waitMsg.Waitmsg
		err = nil
	}

	// No waiting: if the process exited, we kill the connection.
	if me.stdinConn != nil {
		me.stdinConn.Close()
	}

	// We could use a connection here too, but this is simpler.
	me.rep.Stdout = stdout.String()
	me.rep.Stderr = stderr.String()

	return err
}

// Sorts FileAttr such deletions come reversed before additions.

func (me *Mirror) fillReply(ufs *fs.MemUnionFs) *attr.FileSet {
	yield := ufs.Reap()
	wrRoot := strings.TrimLeft(me.writableRoot, "/")
	content := me.worker.content

	files := []*attr.FileAttr{}
	reapedHashes := map[string]string{}
	for path, v := range yield {
		f := &attr.FileAttr{
			Path: filepath.Join(wrRoot, path),
		}

		if v.Attr != nil {
			f.Attr = v.Attr
		}
		f.Link = v.Link
		if !f.Deletion() && f.IsRegular() {
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
					timings := me.rpcFs.timings

					start := time.Now()
					f.Hash, err = content.DestructiveSavePath(v.Backing)
					dt := time.Now().Sub(start)
					timings.Log("Store.DestructiveSavePath", dt)
					timings.LogN("Store.DestructiveSavePathBytes", int64(f.Size), dt)

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
