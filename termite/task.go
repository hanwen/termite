package termite

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"path/filepath"
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
	fsState, err := t.mirror.newFs(t)
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
	err = t.runInFuse(fsState)
	t.mirror.worker.stats.Exit("fuse")

	t.mirror.worker.stats.Enter("reap")
	if t.mirror.considerReap(fsState, t) {
		t.rep.FileSet, t.rep.TaskIds, t.rep.Reads = t.mirror.reapFuse(fsState)
	} else {
		t.mirror.returnFS(fsState)
	}
	if !t.req.TrackReads {
		// TODO - don't even collect this data if TrackReads is unset.
		t.rep.Reads = nil
	}
	t.mirror.worker.stats.Exit("reap")

	return err
}

func (t *WorkerTask) runInFuse(state *workerFSState) error {
	state.fs.SetDebug(t.req.Debug)
	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}

	dir, err := ioutil.TempDir("", "sandbox")
	if err != nil {
		return err
	}

	args := []string{t.mirror.worker.options.Mkbox,
		"-q",
		"-B", t.req.Binary,
		"-s", dir,
		"-b", "/sys=sys",
		"-b", "/proc=proc",
		"-t", "dev",
		"-b", "/dev/null=dev/null",
		"-b", "/dev/zero=dev/zero",
		"-b", "/dev/urandom=dev/urandom", // maybe should use zero for determinism?
	}

	entries, code := state.fs.fuseFS.rpcFS.OpenDir("", nil)
	if !code.Ok() {
		return fmt.Errorf("OpenDir: %v", code)
	}

	// We can't mount root directly, so we mount the subdirs of the root.
	for _, e := range entries {
		if e.Name == "proc" || e.Name == "sys" || e.Name == "dev" || e.Name == "tmp" ||
			e.Name == "lost+found" || e.Name == "root" {
			continue
		}
		args = append(args, "-b", fmt.Sprintf("/%s=%s", filepath.Join(
			state.fs.fuseFS.mount, e.Name), e.Name),
			"-r", e.Name)
	}

	wrRootMount := fmt.Sprintf("/%s=%s",
		filepath.Join(state.fs.fuseFS.mount, state.fs.id),
		state.fs.fuseFS.writableRoot)

	// setup the writable root.
	if strings.HasPrefix(state.fs.fuseFS.writableRoot, "tmp") {
		// we're in a test: use the system's /tmp dir
		args = append(args,
			"-b", "/tmp=tmp",
			"-b", "/var/tmp=var/tmp",
			"-b", wrRootMount)
	} else {
		args = append(args,
			"-b", wrRootMount,
			// temp dirs must be last, because they might hold the
			// FUSE mountpoint and we don't want to hide that.
			"-t", "tmp",
			"-t", "var/tmp")
	}

	args = append(args,
		"-d", t.req.Dir,
		"-u", "3333",
		"-g", "3333",
		"-r", "/dev",
		"-r", "/sys",
	)

	args = append(args, t.req.Argv...)
	t.cmd = &exec.Cmd{
		Path: args[0],
		Args: args,
	}
	cmd := t.cmd
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
		printCmd, cmd.Dir, state.fs.id)
	err = cmd.Wait()
	if exitErr, ok := err.(*exec.ExitError); ok {
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
func (t *Mirror) fillReply(state *workerFSState) (*attr.FileSet, []string) {
	fsResult := state.fs.reap()
	t.returnFS(state)

	files := make([]*attr.FileAttr, 0, len(fsResult.files))
	wrRoot := strings.TrimLeft(state.fs.fuseFS.writableRoot, "/")
	reapedHashes := map[string]string{}
	for path, v := range fsResult.files {
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
	err := os.Remove(fsResult.dir)
	if err != nil {
		log.Fatalf("fillReply: Remove failed: %v", err)
	}

	return &fset, fsResult.reads
}
