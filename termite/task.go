package termite

import (
	"bytes"
	"exec"
	"fmt"
	"github.com/hanwen/go-fuse/fuse"
	"io/ioutil"
	"log"
	"net"
	"os"
	"path/filepath"
	"strings"
	"syscall"
)

type WorkerTask struct {
	fuseFs *WorkerFuseFs
	*WorkRequest
	*WorkReply
	stdinConn net.Conn
	mirror    *Mirror

	taskInfo string
}

func (me *WorkerTask) Run() os.Error {
	me.fuseFs.SetDebug(me.WorkRequest.Debug)
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
		attr.Chroot = me.fuseFs.mount

		cmd.SysProcAttr = attr
		cmd.Dir = me.WorkRequest.Dir
	} else {
		cmd.Path = filepath.Join(me.fuseFs.mount, me.WorkRequest.Binary)
		cmd.Dir = filepath.Join(me.fuseFs.mount, me.WorkRequest.Dir)
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
	log.Println("started cmd", printCmd, "in", me.fuseFs.mount)
	me.taskInfo = fmt.Sprintf("Cmd %v, dir %v, proc %v", cmd.Args, cmd.Dir, cmd.Process)
	err := cmd.Wait()
	waitMsg, ok := err.(*os.Waitmsg)
	if ok {
		me.WorkReply.Exit = *waitMsg
		err = nil
	}

	// No waiting: if the process exited, we kill the connection.
	if me.stdinConn != nil {
		me.stdinConn.Close()
	}

	// We could use a connection here too, but this is simpler.
	me.WorkReply.Stdout = stdout.String()
	me.WorkReply.Stderr = stderr.String()

	err = me.fillReply()
	if err != nil {
		log.Println("discarding FUSE due to error:", me.fuseFs.mount, err)
		me.mirror.discardFuse(me.fuseFs)
	} else {
		// Must do updateFiles before ReturnFuse, since the
		// next job should not see out-of-date files.
		me.mirror.updateFiles(me.WorkReply.Files)
		me.mirror.returnFuse(me.fuseFs)
	}

	return err
}

func (me *WorkerTask) fillReply() os.Error {
	saver := &fileSaver{
		rwDir:  me.fuseFs.rwDir,
		prefix: me.mirror.writableRoot,
		cache:  me.mirror.daemon.contentCache,
	}
	saver.reapBackingStore()
	me.WorkReply.Files = saver.files

	return saver.err
}

type fileSaver struct {
	rwDir  string
	prefix string
	err    os.Error
	files  []FileAttr
	cache  *ContentCache
}

func (me *fileSaver) VisitFile(path string, osInfo *os.FileInfo) {
	me.savePath(path, osInfo)
}

func (me *fileSaver) VisitDir(path string, osInfo *os.FileInfo) bool {
	me.savePath(path, osInfo)
	return me.err == nil
}

func (me *fileSaver) savePath(path string, osInfo *os.FileInfo) {
	if me.err != nil {
		return
	}
	if !strings.HasPrefix(path, me.rwDir) {
		log.Println("Weird file", path)
		return
	}

	fi := FileAttr{
		FileInfo: osInfo,
		Path:     path[len(me.rwDir):],
	}
	if !strings.HasPrefix(fi.Path, me.prefix) || fi.Path == "/"+_DELETIONS {
		return
	}

	ftype := osInfo.Mode &^ 07777
	switch ftype {
	case fuse.S_IFDIR:
		// nothing.
	case fuse.S_IFREG:
		fi.Hash = me.cache.DestructiveSavePath(path)
		if fi.Hash == "" {
			me.err = os.NewError("DestructiveSavePath fail")
		}
		// TODO - optionally save contents too? Should cap
		// with a limit to avoid choking on mv of large
		// directory.
	case fuse.S_IFLNK:
		val, err := os.Readlink(path)
		me.err = err
		fi.Link = val
		os.Remove(path)
	default:
		log.Fatalf("Unknown file type %o", ftype)
	}

	me.files = append(me.files, fi)
}

func (me *fileSaver) reapBackingStore() {
	dir := filepath.Join(me.rwDir, _DELETIONS)
	_, err := os.Lstat(dir)
	if err == nil {
		matches, err := filepath.Glob(dir + "/*")
		if err != nil {
			me.err = err
			return
		}

		for _, fullPath := range matches {
			contents, err := ioutil.ReadFile(fullPath)
			if err != nil {
				me.err = err
				return
			}

			me.files = append(me.files, FileAttr{
				Status: fuse.ENOENT,
				Path:   "/" + string(contents),
			})
			me.err = os.Remove(fullPath)
			if me.err != nil {
				break
			}
		}
	}

	if me.err == nil {
		filepath.Walk(me.rwDir, me, nil)
	}

	for i, _ := range me.files {
		if me.err != nil {
			break
		}
		f := me.files[len(me.files)-i-1]
		if f.FileInfo != nil && f.FileInfo.IsDirectory() && f.Path != me.prefix {
			me.err = os.Remove(filepath.Join(me.rwDir, f.Path))
		}
	}
}
