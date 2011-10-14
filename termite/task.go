package termite

import (
	"bytes"
	"exec"
	"fmt"
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
	cmd       *exec.Cmd 
	taskInfo string
}

func (me *WorkerTask) Kill() {
	// TODO - racy.
	if me.cmd.Process != nil {
		pid := me.cmd.Process.Pid
		errNo := syscall.Kill(pid, syscall.SIGQUIT)
		log.Printf("Killed pid %d, result %d", pid, errNo)
	}
}

func (me *WorkerTask) String() string {
	return me.taskInfo
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
	fuseFs, err := me.mirror.newFs(me)
	if err != nil {
		return err
	}

	me.resetClock()
	err = me.runInFuse(fuseFs)
	if err != nil {
		return err
	}
	if me.mirror.considerReap(fuseFs, me) {
		me.WorkResponse.FileSet, me.WorkResponse.TaskIds = me.mirror.reapFuse(fuseFs)
	}

	me.mirror.returnFs(fuseFs)
	me.clock("worker.returnFuse")
	return nil
}

func (me *WorkerTask) runInFuse(fuseFs *workerFuseFs) os.Error {
	fuseFs.SetDebug(me.WorkRequest.Debug)
	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}

	me.cmd = exec.Command(me.WorkRequest.Binary,
		me.WorkRequest.Argv[1:]...)
	cmd := me.cmd
	
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

	printCmd := fmt.Sprintf("%v", cmd.Args)
	if me.WorkRequest.Debug {
		printCmd = fmt.Sprintf("%v", cmd)
	}
	me.taskInfo = fmt.Sprintf("%v, dir %v, fuse FS %d",
		printCmd, cmd.Dir, fuseFs.id)
	log.Println("started", me.taskInfo)
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
	return err
}

// Sorts FileAttr such deletions come reversed before additions.

func (me *Mirror) fillReply(ufs *unionfs.MemUnionFs) *FileSet {
	yield := ufs.Reap()
	wrRoot := strings.TrimLeft(me.writableRoot, "/")
	cache := me.daemon.contentCache

	files := []*FileAttr{}
	reapedHashes := map[string]string{}
	for path, v := range yield {
		f := &FileAttr{
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
				var err os.Error
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
	fs := FileSet{files}
	ufs.Clear()
	fs.Sort()

	return &fs
}
