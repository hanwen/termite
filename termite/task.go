package termite

import (
	"bytes"
	"exec"
	"fmt"
	"github.com/hanwen/go-fuse/fuse"
	"github.com/hanwen/go-fuse/unionfs"
	"log"
	"net"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

type WorkerTask struct {
	*WorkRequest
	*WorkResponse
	stdinConn net.Conn
	mirror    *Mirror

	taskInfo string

	lastTime int64
}

func (me *WorkResponse) resetClock() {
	me.LastTime = time.Nanoseconds()
}

func (me *WorkResponse) clock(name string) {
	t := time.Nanoseconds()
	me.Timings = append(me.Timings,
		Timing{name, 1.0e-6 * float64(t-me.LastTime)})
	me.LastTime = t
}

func (me *WorkerTask) clock(name string) {
	me.WorkResponse.clock(name)
}

func (me *WorkerTask) Run() os.Error {
	me.resetClock()
	fuseFs, err := me.mirror.getWorkerFuseFs(me.WorkRequest.Summary())
	if err != nil {
		return err
	}
	me.clock("worker.getWorkerFuseFs")

	fuseFs.task = me
	err = me.runInFuse(fuseFs)
	if err != nil {
		log.Println("discarding FUSE due to error:", fuseFs.mount, err)
		me.mirror.discardFuse(fuseFs)
		return err
	}

	me.mirror.returnFuse(fuseFs)
	me.clock("worker.returnFuse")
	return nil
}

func (me *WorkerTask) runInFuse(fuseFs *workerFuseFs) os.Error {
	fuseFs.SetDebug(me.WorkRequest.Debug)
	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}

	cmd := exec.Command(me.WorkRequest.Binary,
		me.WorkRequest.Argv[1:]...)
	cmd.Args[0] = me.WorkRequest.Argv[0]
	if os.Geteuid() == 0 {
		attr := &syscall.SysProcAttr{}
		attr.Credential = &syscall.Credential{
			Uid: uint32(me.mirror.daemon.Nobody.Uid),
			Gid: uint32(me.mirror.daemon.Nobody.Gid),
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

	// Must modify printCmd after starting the process.
	printCmd := cmd
	if !me.WorkRequest.Debug {
		printCmd.Env = nil
	}
	log.Println("started cmd", printCmd, "in", fuseFs.mount)
	me.taskInfo = fmt.Sprintf("Cmd %v, dir %v, proc %v", cmd.Args, cmd.Dir, cmd.Process)
	err := cmd.Wait()

	waitMsg, ok := err.(*os.Waitmsg)
	if ok {
		me.WorkResponse.Exit = *waitMsg
		err = nil
	}

	// No waiting: if the process exited, we kill the connection.
	if me.stdinConn != nil {
		me.stdinConn.Close()
	}

	// We could use a connection here too, but this is simpler.
	me.WorkResponse.Stdout = stdout.String()
	me.WorkResponse.Stderr = stderr.String()

	me.clock("worker.runCommand")
	err = me.fillReply(fuseFs.unionFs)
	me.clock("worker.fillReply")
	if err == nil {
		// Must do updateFiles before ReturnFuse, since the
		// next job should not see out-of-date files.
		me.mirror.updateFiles(me.WorkResponse.Files, fuseFs)
		me.clock("worker.updateFiles")
	}

	return err
}

func (me *WorkerTask) fillReply(ufs *unionfs.MemUnionFs) os.Error {
	yield := ufs.Reap()
	ufs.Clear()
	wrRoot := strings.TrimLeft(me.mirror.writableRoot, "/")
	cache := me.mirror.daemon.contentCache

	files := []*FileAttr{}
	for path, v := range yield {
		f := &FileAttr{
			Path: "/" + filepath.Join(wrRoot, path),
		}

		if v.FileInfo == nil  {
			f.Status = fuse.ENOENT
		} else {
			f.FileInfo = v.FileInfo
			f.Link = v.Link
			if f.FileInfo.IsRegular() {
				if v.Original != "" {
					contentPath := filepath.Join(wrRoot, v.Original)
					fa := me.mirror.rpcFs.getFileAttr(contentPath)
					if fa.Hash == "" {
						panic(fmt.Sprintf("Contents for %q disappeared.", contentPath))
					}
					f.Hash = fa.Hash
				} else {
					f.Hash = cache.DestructiveSavePath(v.Backing)
					if f.Hash == "" {
						return os.NewError(fmt.Sprintf("DestructiveSavePath fail %q", v.Backing))
					}
				}
			}
		}

		files = append(files, f)
	}
	me.WorkResponse.Files = files
	return nil
}
