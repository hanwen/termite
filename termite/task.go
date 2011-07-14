package termite

// TODO - this list of imports is scary; split up?

import (
	"bytes"
	"fmt"
	"path/filepath"
	"os"
	"log"
	"net"
	"io/ioutil"
	"io"
	"github.com/hanwen/go-fuse/fuse"
	"os/user"
	"strings"
	"syscall"
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

	nobody, err := user.Lookup("nobody")
	if err != nil {
		return err
	}

	cmd := []string{}
	binary := ""
	if os.Geteuid() == 0 {
		binary := me.mirror.daemon.ChrootBinary
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

	log.Println("starting cmd", cmd)
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
	go func() {
		HookedCopy(wStdin, me.stdinConn, PrintStdinSliceLen)
		// No waiting: if the process exited, we kill the connection.
		wStdin.Close()
	}()

	me.WorkReply.Exit, err = proc.Wait(0)
	wg.Wait()

	// No waiting: if the process exited, we kill the connection.
	me.stdinConn.Close()

	// TODO - should use a connection here too? What if the output
	// is large?
	me.WorkReply.Stdout = stdout.String()
	me.WorkReply.Stderr = stderr.String()

	err = me.fillReply()
	if err != nil {
		// TODO - anything else needed to discard?
		log.Println("discarding FUSE due to error:", err)
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
	saver.scanBackingStore()
	me.WorkReply.Files = saver.files
	return saver.err
}

type fileSaver struct {
	rwDir  string
	prefix string
	err    os.Error
	files  []AttrResponse
	cache  *DiskFileCache
}

func (me *fileSaver) VisitFile(path string, osInfo *os.FileInfo) {
	me.savePath(path, osInfo)
}

func (me *fileSaver) VisitDir(path string, osInfo *os.FileInfo) bool {
	me.savePath(path, osInfo)

	// TODO - save dir to delete.
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

	fi := AttrResponse{
		FileInfo: osInfo,
		Path:     path[len(me.rwDir):],
	}
	if !strings.HasPrefix(fi.Path, me.prefix) || fi.Path == "/"+_DELETIONS {
		return
	}

	ftype := osInfo.Mode &^ 07777
	switch ftype {
	case syscall.S_IFDIR:
		// nothing.
		// TODO - remove dir.
	case syscall.S_IFREG:
		fi.Hash, fi.Content = me.cache.DestructiveSavePath(path)
		if fi.Hash == nil {
			me.err = os.NewError("DestructiveSavePath fail")
		}
	case syscall.S_IFLNK:
		val, err := os.Readlink(path)
		me.err = err
		fi.Link = val
		os.Remove(path)
	default:
		log.Fatalf("Unknown file type %o", ftype)
	}

	me.files = append(me.files, fi)
}

func (me *fileSaver) scanBackingStore() os.Error {
	dir := filepath.Join(me.rwDir, _DELETIONS)
	_, err := os.Lstat(dir)
	if err == nil {
		matches, err := filepath.Glob(dir + "/*")
		if err != nil {
			return err
		}

		for _, fullPath := range matches {
			contents, err := ioutil.ReadFile(fullPath)
			if err != nil {
				return err
			}

			me.files = append(me.files, AttrResponse{
				Status: fuse.ENOENT,
				Path:   string(contents),
			})
			err = os.Remove(fullPath)
			if err != nil {
				return err
			}
		}

		if err != nil {
			return err
		}
	}

	filepath.Walk(me.rwDir, me, nil)
	return nil
}
