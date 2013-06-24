package termite

import (
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/hanwen/go-fuse/fuse"
	"github.com/hanwen/go-fuse/fuse/nodefs"
)

var _ = log.Println

type lazyLoopbackFile struct {
	nodefs.File

	mu   sync.Mutex
	f    nodefs.File
	Name string
}

func NewLazyLoopbackFile(n string) nodefs.File {
	return &lazyLoopbackFile{
		File: nodefs.NewDefaultFile(),
		Name: n,
	}
}

func (me *lazyLoopbackFile) file() (nodefs.File, fuse.Status) {
	me.mu.Lock()
	defer me.mu.Unlock()
	if me.f == nil {
		f, err := os.Open(me.Name)
		if err != nil {
			return nil, fuse.ToStatus(err)
		}
		me.f = nodefs.NewLoopbackFile(f)
	}
	return me.f, fuse.OK
}

func (me *lazyLoopbackFile) InnerFile() nodefs.File {
	f, _ := me.file()
	return f
}

func (me *lazyLoopbackFile) String() string {
	return fmt.Sprintf("lazyLoopbackFile(%s)", me.Name)
}

func (me *lazyLoopbackFile) Read(buf []byte, off int64) (fuse.ReadResult, fuse.Status) {
	f, s := me.file()
	if s.Ok() {
		return f.Read(buf, off)
	}
	return nil, fuse.OK
}

func (me *lazyLoopbackFile) Release() {
	me.mu.Lock()
	defer me.mu.Unlock()
	if me.f != nil {
		me.f.Release()
	}
}

func (me *lazyLoopbackFile) Write(s []byte, off int64) (uint32, fuse.Status) {
	return 0, fuse.EPERM
}

func (me *lazyLoopbackFile) GetAttr(a *fuse.Attr) fuse.Status {
	f, s := me.file()
	if s.Ok() {
		return f.GetAttr(a)
	}
	return s
}

func (me *lazyLoopbackFile) Utimens(atimeNs, mtimeNs *time.Time) fuse.Status {
	return fuse.EPERM
}

func (me *lazyLoopbackFile) Truncate(size uint64) fuse.Status {
	return fuse.EPERM
}

func (me *lazyLoopbackFile) Chown(uid uint32, gid uint32) fuse.Status {
	return fuse.EPERM
}

func (me *lazyLoopbackFile) Chmod(perms uint32) fuse.Status {
	return fuse.EPERM
}
