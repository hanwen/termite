package termite

// TODO - this list of imports is scary; split up?

import (
	"bytes"
	"exec"
	"fmt"
	"github.com/hanwen/go-fuse/fuse"
	"io/ioutil"
	"log"
	"net"
	"os"
	"os/user"
	"path/filepath"
	"strings"
)

type WorkerTask struct {
	fuseFs *WorkerFuseFs
	*WorkRequest
	*WorkReply
	stdinConn net.Conn
	mirror    *Mirror
}

func (me *WorkerTask) Run() os.Error {
	me.fuseFs.MountState.Debug = me.WorkRequest.Debug
	me.fuseFs.fsConnector.Debug = me.WorkRequest.Debug

	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}
	
	args := []string{}
	binary := ""
	dir := ""
	if os.Geteuid() == 0 {
		nobody, err := user.Lookup("nobody")
		if err != nil {
			return err
		}
		// TODO - use SysProcAttr.Credential/Chroot instead.
		binary = me.mirror.daemon.ChrootBinary
		args = append(args, binary, "-dir", me.WorkRequest.Dir,
			"-uid", fmt.Sprintf("%d", nobody.Uid), "-gid", fmt.Sprintf("%d", nobody.Gid),
			"-binary", me.WorkRequest.Binary,
			me.fuseFs.mount)
		args = append(args, me.WorkRequest.Argv...)
	} else {
		args = me.WorkRequest.Argv
		binary = me.WorkRequest.Argv[0]
		dir = filepath.Join(me.fuseFs.mount, me.WorkRequest.Dir)
		log.Println("running in", dir)
	}

	cmd := exec.Command(args[0], args[1:]...)
	cmd.Path = binary
	cmd.Env = me.WorkRequest.Env
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	cmd.Dir = dir
	if me.stdinConn != nil {
		cmd.Stdin = me.stdinConn
	}

	printCmd := cmd
	printCmd.Env = nil
	log.Println("starting cmd", printCmd, "in", me.fuseFs.mount)

	err := cmd.Run()
	waitMsg, ok := err.(*os.Waitmsg)
	if ok {
		me.WorkReply.Exit = waitMsg
		err = nil
	} else {
		// TODO - use struct instead?
		me.WorkReply.Exit = &os.Waitmsg{}
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
		// TODO - remove dir.
	case fuse.S_IFREG:
		fi.Hash, fi.Content = me.cache.DestructiveSavePath(path)
		if fi.Hash == nil {
			me.err = os.NewError("DestructiveSavePath fail")
		}
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
