package termite

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"strings"
	"syscall"

	"github.com/hanwen/termite/attr"
	"github.com/hanwen/termite/fastpath"
)

type WorkerTask struct {
	req       *WorkRequest
	rep       *WorkResponse
	stdinConn io.ReadWriteCloser
	mirror    *Mirror
	cmd       *exec.Cmd
	taskInfo  string
}

func (t *WorkerTask) Kill() {
	if t.cmd.Process != nil {
		pid := t.cmd.Process.Pid
		err := syscall.Kill(pid, syscall.SIGQUIT)
		log.Printf("Killed pid %d, result %v", pid, err)
	}
}

func (t *WorkerTask) String() string {
	return t.taskInfo
}

func (t *WorkerTask) Run() error {
	fuseFS, err := t.mirror.newFs(t)

	if err == ShuttingDownError {
		// We can't return an error, since that would cause
		// the master to drop us directly before the other
		// jobs finished, opening a time window where there
		// might no worker running at all.
		//
		// TODO - some gentler signaling to the master?
		select {}
	}

	if err != nil {
		return err
	}

	t.mirror.worker.stats.Enter("fuse")
	err = t.runInFuse(fuseFS)
	t.mirror.worker.stats.Exit("fuse")

	t.mirror.worker.stats.Enter("reap")
	if t.mirror.considerReap(fuseFS, t) {
		t.rep.FileSet, t.rep.TaskIds = t.mirror.reapFuse(fuseFS)
	} else {
		t.mirror.returnFS(fuseFS)
	}
	t.mirror.worker.stats.Exit("reap")

	return err
}

func (t *WorkerTask) runInFuse(state *workerFSState) error {
	state.fs.SetDebug(t.req.Debug)
	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}

	// See /bin/true for the background of
	// /bin/true. http://code.google.com/p/go/issues/detail?id=2373
	t.cmd = &exec.Cmd{
		Path: t.req.Binary,
		Args: t.req.Argv,
	}
	cmd := t.cmd
	if os.Geteuid() == 0 {
		attr := &syscall.SysProcAttr{}
		attr.Credential = &syscall.Credential{
			Uid: uint32(t.mirror.worker.options.User.Uid),
			Gid: uint32(t.mirror.worker.options.User.Gid),
		}
		attr.Chroot = state.fs.rootDir
		cmd.SysProcAttr = attr
		cmd.Dir = t.req.Dir
	} else {
		cmd.Path = fastpath.Join(state.fs.rootDir, t.req.Binary)
		cmd.Dir = fastpath.Join(state.fs.rootDir, t.req.Dir)
	}

	cmd.Env = t.req.Env
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	if t.stdinConn != nil {
		cmd.Stdin = t.stdinConn
	}

	if err := cmd.Start(); err != nil {
		return err
	}

	printCmd := fmt.Sprintf("%v", cmd.Args)
	if t.req.Debug {
		printCmd = fmt.Sprintf("%v", cmd)
	}
	t.taskInfo = fmt.Sprintf("%v, dir %v, fuse FS %v",
		printCmd, cmd.Dir, state.id)
	err := cmd.Wait()

	exitErr, ok := err.(*exec.ExitError)
	if ok {
		t.rep.Exit = exitErr.Sys().(syscall.WaitStatus)
		err = nil
	}

	// No waiting: if the process exited, we kill the connection.
	if t.stdinConn != nil {
		t.stdinConn.Close()
	}

	// We could use a connection here too, but this is simpler.
	t.rep.Stdout = stdout.String()
	t.rep.Stderr = stderr.String()

	return err
}

// fillReply empties the unionFs and hashes files as needed.  It will
// return the FS back the pool as soon as possible.
func (t *Mirror) fillReply(state *workerFSState) *attr.FileSet {
	dir, yield := state.fs.reap()
	t.returnFS(state)

	files := make([]*attr.FileAttr, 0, len(yield))
	wrRoot := strings.TrimLeft(state.fs.fuseFS.writableRoot, "/")
	reapedHashes := map[string]string{}
	for path, v := range yield {
		f := &attr.FileAttr{
			Path: fastpath.Join(wrRoot, path),
		}

		if v.Attr != nil {
			f.Attr = v.Attr
		}
		f.Link = v.Link
		if !f.Deletion() && f.IsRegular() {
			contentPath := fastpath.Join(wrRoot, v.Original)
			if v.Original != "" && v.Original != contentPath {
				fa := t.rpcFs.attr.Get(contentPath)
				if fa.Hash == "" {
					log.Panicf("Contents for %q disappeared.", contentPath)
				}
				f.Hash = fa.Hash
			}
			if v.Backing != "" {
				h, ok := reapedHashes[v.Backing]
				if !ok {
					var err error

					h, err = t.worker.content.DestructiveSavePath(v.Backing)
					if err != nil || h == "" {
						log.Fatalf("DestructiveSavePath fail %v, %q", err, h)
					}
					reapedHashes[v.Backing] = h
				}
				f.Hash = h
			}
		}
		files = append(files, f)
	}

	fset := attr.FileSet{Files: files}
	fset.Sort()
	err := os.Remove(dir)
	if err != nil {
		log.Fatal("fillReply: Remove failed: %v", err)
	}

	return &fset
}
