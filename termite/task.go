package termite

// TODO - this list of imports is scary; split up?

import (
	"bytes"
	"fmt"
	"github.com/hanwen/go-fuse/fuse"
	"io"
	"io/ioutil"
	"log"
	"net"
	"os"
	"os/user"
	"path/filepath"
	"strings"
	"sync"
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

	rStdout, wStdout, err := os.Pipe()
	if err != nil {
		return err
	}
	rStderr, wStderr, err := os.Pipe()
	if err != nil {
		return err
	}
	rStdin, wStdin, err := os.Pipe()
	if err != nil {
		return err
	}

	attr := os.ProcAttr{
		Env:   me.WorkRequest.Env,
		Files: []*os.File{rStdin, wStdout, wStderr},
	}

	cmd := []string{}
	binary := ""
	if os.Geteuid() == 0 {
		nobody, err := user.Lookup("nobody")
		if err != nil {
			return err
		}
		binary = me.mirror.daemon.ChrootBinary
		cmd = []string{binary, "-dir", me.WorkRequest.Dir,
			"-uid", fmt.Sprintf("%d", nobody.Uid), "-gid", fmt.Sprintf("%d", nobody.Gid),
			"-binary", me.WorkRequest.Binary,
			me.fuseFs.mount}

		newcmd := make([]string, len(cmd)+len(me.WorkRequest.Argv))
		copy(newcmd, cmd)
		copy(newcmd[len(cmd):], me.WorkRequest.Argv)

		cmd = newcmd
	} else {
		cmd = me.WorkRequest.Argv
		binary = me.WorkRequest.Argv[0]
		attr.Dir = filepath.Join(me.fuseFs.mount, me.WorkRequest.Dir)
		log.Println("running in", attr.Dir)
	}

	log.Println("starting cmd", cmd, "in", me.fuseFs.mount)
	proc, err := os.StartProcess(binary, cmd, &attr)
	if err != nil {
		log.Println("Error", err)
		return err
	}

	wStdout.Close()
	wStderr.Close()
	rStdin.Close()

	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		io.Copy(stdout, rStdout)
		wg.Done()
	}()
	go func() {
		io.Copy(stderr, rStderr)
		wg.Done()
	}()

	if me.stdinConn != nil {
		go func() {
			HookedCopy(wStdin, me.stdinConn, PrintStdinSliceLen)
			// No waiting: if the process exited, we kill the connection.
			wStdin.Close()
		}()
	} else {
		wStdin.Close()
	}

	me.WorkReply.Exit, err = proc.Wait(0)
	wg.Wait()

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
		me.mirror.DiscardFuse(me.fuseFs)
	} else {
		me.mirror.ReturnFuse(me.fuseFs)
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
	if false {
		// TODO.
		if me.err == nil {
			me.err = os.RemoveAll(me.rwDir)
		}
		if me.err == nil {
			me.err = os.Mkdir(me.rwDir, 0755)
		}
		if me.err == nil {
			me.err = os.Mkdir(filepath.Join(me.rwDir, _DELETIONS), 0755)
		}
	}
}
