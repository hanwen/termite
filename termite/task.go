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
	"sort"
	"syscall"
	"time"
)

type WorkerTask struct {
	*WorkRequest
	*WorkResponse
	stdinConn net.Conn
	mirror    *Mirror

	taskInfo string
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
		me.WorkResponse.FileSet = me.mirror.reapFuse(fuseFs)
	}

	me.mirror.returnFs(fuseFs)
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
	return err
}

// Sorts FileAttr such deletions come reversed before additions.
type sortableFiles []*FileAttr
func (me sortableFiles) Len() int {
	return len(me)
}

func (me sortableFiles) Less(i, j int) bool {
	a := me[i]
	b := me[j]
	if a.Deletion() != b.Deletion() {
		return a.Deletion()
	}

	if a.Deletion() {
		return a.Path > b.Path
	} 
	return a.Path < b.Path
}

func (me sortableFiles) Swap(i, j int) {
	me[i], me[j] = me[j], me[i]  
}

func (me sortableFiles) Sort() {
	sort.Sort(me)
}

func (me *Mirror) fillReply(ufs *unionfs.MemUnionFs) *FileSet {
	yield := ufs.Reap()
	wrRoot := strings.TrimLeft(me.writableRoot, "/")
	cache := me.daemon.contentCache

	files := sortableFiles{}
	for path, v := range yield {
		f := &FileAttr{
			Path: filepath.Join(wrRoot, path),
		}

		if v.FileInfo == nil  {
			f.Status = fuse.ENOENT
		} else {
			f.FileInfo = v.FileInfo
			f.Link = v.Link
			if f.FileInfo.IsRegular() {
				if v.Original != "" {
					contentPath := filepath.Join(wrRoot, v.Original)
					fa := me.rpcFs.getFileAttr(contentPath)
					if fa.Hash == "" {
						log.Panicf("Contents for %q disappeared.", contentPath)
					}
					f.Hash = fa.Hash
				} else {
					f.Hash = cache.DestructiveSavePath(v.Backing)
					if f.Hash == "" {
						log.Fatalf("DestructiveSavePath fail %q", v.Backing)
					}
				}
			}
		}

		files = append(files, f)
	}
	ufs.Clear()
	sort.Sort(files)
	
	return &FileSet{([]*FileAttr)(files)}
}
