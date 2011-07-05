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
	"github.com/hanwen/go-fuse/unionfs"
	"os/user"
	"strings"
	"syscall"
	"sync"
)

type WorkerFuseFs struct {
	rwDir  string
	tmpDir string
	mount  string
	*fuse.MountState
	unionFs *unionfs.UnionFs
}

type WorkerTask struct {
	fuseFs *WorkerFuseFs
	*WorkRequest
	*WorkReply
	stdinConn    net.Conn
	masterWorker *Mirror
}

func (me *WorkerFuseFs) Stop() {
	me.MountState.Unmount()
	os.RemoveAll(me.tmpDir)
}


func (me *WorkerTask) Run() os.Error {
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

	chroot := me.masterWorker.daemon.ChrootBinary
	cmd := []string{chroot, "-dir", me.WorkRequest.Dir,
		"-uid", fmt.Sprintf("%d", nobody.Uid), "-gid", fmt.Sprintf("%d", nobody.Gid),
		"-binary", me.WorkRequest.Binary,
		me.fuseFs.mount}

	newcmd := make([]string, len(cmd)+len(me.WorkRequest.Argv))
	copy(newcmd, cmd)
	copy(newcmd[len(cmd):], me.WorkRequest.Argv)

	log.Println("starting cmd", newcmd)
	proc, err := os.StartProcess(chroot, newcmd, &attr)
	if err != nil {
		log.Println("Error", err)
		return err
	}

	wStdout.Close()
	wStderr.Close()

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
		me.stdinConn.Close()
	}()

	me.WorkReply.Exit, err = proc.Wait(0)
	wg.Wait()

	// TODO - should use a connection here too? What if the output
	// is large?
	me.WorkReply.Stdout = stdout.String()
	me.WorkReply.Stderr = stderr.String()

	// TODO - look at rw directory, and serialize the files into WorkReply.
	err = me.fillReply()
	if err != nil {
		// TODO - anything else needed to discard?
		me.masterWorker.DiscardFuse(me.fuseFs)
	} else {
		me.masterWorker.ReturnFuse(me.fuseFs)
	}

	return err
}

func (me *WorkerTask) VisitFile(path string, osInfo *os.FileInfo) {
	fi := AttrResponse{
		FileInfo: osInfo,
	}

	ftype := osInfo.Mode &^ 07777
	switch ftype {
	case syscall.S_IFREG:
		fi.Hash = me.masterWorker.daemon.contentCache.SavePath(path)
	case syscall.S_IFLNK:
		val, err := os.Readlink(path)
		if err != nil {
			// TODO - fail rpc.
			log.Fatal("Readlink error.")
		}
		fi.Link = val
	default:
		log.Fatalf("Unknown file type %o", ftype)
	}

	me.savePath(path, fi)

	// TODO - error handling.
	os.Remove(path)
}

func (me *WorkerTask) savePath(path string, fi AttrResponse) {
	if !strings.HasPrefix(path, me.fuseFs.rwDir) {
		log.Println("Weird file", path)
		return
	}

	fi.Path = path[len(me.fuseFs.rwDir):]
	if fi.Path == "/"+_DELETIONS {
		return
	}

	me.WorkReply.Files = append(me.WorkReply.Files, fi)
}

func (me *WorkerTask) VisitDir(path string, osInfo *os.FileInfo) bool {
	me.savePath(path, AttrResponse{FileInfo: osInfo})

	// TODO - save dir to delete.
	return true
}

func (me *WorkerTask) fillReply() os.Error {
	dir := filepath.Join(me.fuseFs.rwDir, _DELETIONS)
	_, err := os.Lstat(dir)
	if err == nil {
		matches, err := filepath.Glob(dir + "/*")
		if err != nil {
			return err
		}

		for _, m := range matches {
			fullPath := filepath.Join(dir, m)
			contents, err := ioutil.ReadFile(fullPath)
			if err != nil {
				return err
			}

			me.WorkReply.Files = append(me.WorkReply.Files, AttrResponse{
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
	filepath.Walk(me.fuseFs.rwDir, me, nil)
	return nil
}
