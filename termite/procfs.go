package termite

import (
	"fmt"
	"github.com/hanwen/go-fuse/fuse"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"
	"unicode"
)

var _ = log.Println

// Read files from proc - since they have 0 size, we must read the
// file to set the size correctly.
type ProcFs struct {
	*fuse.LoopbackFileSystem
	StripPrefix string
	AllowedRootFiles map[string]int
	SelfPid int
	Uid     int
}

func NewProcFs() *ProcFs {
	return &ProcFs{
		LoopbackFileSystem: fuse.NewLoopbackFileSystem("/proc"),
		StripPrefix: "/",
		SelfPid: 1,
	}
}

func isNum(n string) bool {
	for _, c := range n {
		if !unicode.IsDigit(c) {
			return false
		}
	}
	return len(n) > 0
}

func (me *ProcFs) GetAttr(name string) (*os.FileInfo, fuse.Status) {
	dir, base := filepath.Split(name)
	dir = filepath.Clean(dir)
	if name != "" && dir == "." && !isNum(name) && me.AllowedRootFiles != nil {
		if _, ok := me.AllowedRootFiles[base]; !ok {
			return nil, fuse.ENOENT
		}
	}

	if isNum(dir) && os.Geteuid() == 0 {
		// We are running as root, so we have to enforce permissions here.
		// TODO - also return EPERM for OpenDir()
		p := me.LoopbackFileSystem.GetPath(dir)
		if fi, err := os.Lstat(p); err != nil || fi.Uid != me.Uid {
			return nil, fuse.EPERM
		}
	}
	
	fi, code := me.LoopbackFileSystem.GetAttr(name)
	if fi != nil && fi.IsRegular() && fi.Size == 0 {
		p := me.LoopbackFileSystem.GetPath(name)
		content, _ := ioutil.ReadFile(p)
		fi.Size = int64(len(content))
	}
	return fi, code
}

func (me *ProcFs) Readlink(name string) (string, fuse.Status) {
	if name == "self" {
		// TODO - this is broken if the process fork()s.
		return fmt.Sprintf("%d", me.SelfPid), fuse.OK
	}
	val, code := me.LoopbackFileSystem.Readlink(name)
	if code.Ok() && strings.HasPrefix(val, me.StripPrefix) {
		val = "/" + strings.TrimLeft(val[len(me.StripPrefix):], "/")
	}
	return val, code
}
