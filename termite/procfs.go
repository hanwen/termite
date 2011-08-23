package termite

import (
	"fmt"
	"github.com/hanwen/go-fuse/fuse"
	"io/ioutil"
	"os"
	"strings"
)

// Expose /proc/self
type ProcFs struct {
	*fuse.LoopbackFileSystem
	Pid int
}

func NewProcFs() *ProcFs {
	return &ProcFs{
		fuse.NewLoopbackFileSystem("/proc"),
		1,
	}
}

func (me *ProcFs) GetAttr(name string) (*os.FileInfo, fuse.Status) {
	if name == "" {
		fi := os.FileInfo{Mode: fuse.S_IFDIR | 0777}
		return &fi, fuse.OK
	}
	if name == "self" {
		fi := os.FileInfo{Mode: fuse.S_IFDIR | 0777}
		return &fi, fuse.OK
	}

	if strings.HasPrefix(name, "self/") {
		name = fmt.Sprintf("%d/%s", me.Pid, name[len("self/"):])
		fi, code := me.LoopbackFileSystem.GetAttr(name)
		if fi != nil && fi.IsRegular() && fi.Size == 0 {
			p := me.LoopbackFileSystem.GetPath(name)
			content, _ := ioutil.ReadFile(p)
			fi.Size = int64(len(content))
		}
		return fi, code
	}
	
	return nil, fuse.ENOENT
}

func (me *ProcFs) OpenDir(name string) (stream chan fuse.DirEntry, status fuse.Status) {
	if name == "" {
		stream := make(chan fuse.DirEntry, 1)
		stream <- fuse.DirEntry{fuse.S_IFDIR | 0666, "self"}
		close(stream)
		return stream, fuse.OK
	}
	if strings.HasPrefix(name, "self") {
		name = fmt.Sprintf("%d/%s", me.Pid, name[len("self"):])
		return me.LoopbackFileSystem.OpenDir(name)
	}
	return nil, fuse.ENOENT
}

func (me *ProcFs) GetXAttr(name string, attr string) ([]byte, fuse.Status) {
	return nil, fuse.ENODATA
}


