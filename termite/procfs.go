package termite

import (
	"fmt"
	"github.com/hanwen/go-fuse/fuse"
	"io/ioutil"
	"log"
	"os"
	"strings"
)

// Read files from proc - since they have 0 size, we must read the
// file to set the size correctly.
type ProcFs struct {
	*fuse.LoopbackFileSystem
	StripPrefix string
}

func NewProcFs() *ProcFs {
	return &ProcFs{
		// Is this a security problem?
		fuse.NewLoopbackFileSystem("/proc/0"),
		"/",
	}
}

func (me *ProcFs) SetPid(pid int) {
	// TODO - racy access.
	me.LoopbackFileSystem.Root = fmt.Sprintf("/proc/%d", pid)
}

func (me *ProcFs) GetAttr(name string) (*os.FileInfo, fuse.Status) {
	fi, code := me.LoopbackFileSystem.GetAttr(name)
	if fi != nil &&  fi.IsRegular() && fi.Size == 0 {
		p := me.LoopbackFileSystem.GetPath(name)
		content, _ := ioutil.ReadFile(p)
		fi.Size = int64(len(content))
	}
	return fi, code
}

func (me *ProcFs) Readlink(name string) (string, fuse.Status) {
	log.Println("Readlink:")
	val, code := me.LoopbackFileSystem.Readlink(name)
	if code.Ok() && strings.HasPrefix(val, me.StripPrefix) {
		val = "/" + strings.TrimLeft(val[len(me.StripPrefix):], "/")
	}
	return val, code
}
