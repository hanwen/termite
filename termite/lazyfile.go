package termite

import (
	"fmt"
	"log"
	"os"
	"sync"

	"github.com/hanwen/go-fuse/fuse"
)

var _ = log.Println

type LazyLoopbackFile struct {
	fuse.DefaultFile

	mu   sync.Mutex
	f    fuse.File
	Name string
}

func (me *LazyLoopbackFile) SetInode(*fuse.Inode) {
}

func (me *LazyLoopbackFile) file() (fuse.File, fuse.Status) {
	me.mu.Lock()
	defer me.mu.Unlock()
	if me.f == nil {
		f, err := os.Open(me.Name)
		if err != nil {
			return nil, fuse.ToStatus(err)
		}
		me.f = &fuse.LoopbackFile{File: f}
	}
	return me.f, fuse.OK
}

func (me *LazyLoopbackFile) InnerFile() fuse.File {
	f, _ := me.file()
	return f
}

func (me *LazyLoopbackFile) String() string {
	return fmt.Sprintf("LazyLoopbackFile(%s)", me.Name)
}

func (me *LazyLoopbackFile) Read(buf []byte, off int64) (fuse.ReadResult, fuse.Status) {
	f, s := me.file()
	if s.Ok() {
		return f.Read(buf, off)
	}
	return nil, fuse.OK
}

func (me *LazyLoopbackFile) Release() {
	me.mu.Lock()
	defer me.mu.Unlock()
	if me.f != nil {
		me.f.Release()
	}
}

func (me *LazyLoopbackFile) Write(s []byte, off int64) (uint32, fuse.Status) {
	return 0, fuse.EPERM
}

func (me *LazyLoopbackFile) GetAttr(a *fuse.Attr) fuse.Status {
	f, s := me.file()
	if s.Ok() {
		return f.GetAttr(a)
	}
	return s
}

func (me *LazyLoopbackFile) Utimens(atimeNs int64, mtimeNs int64) fuse.Status {
	return fuse.EPERM
}

func (me *LazyLoopbackFile) Truncate(size uint64) fuse.Status {
	return fuse.EPERM
}

func (me *LazyLoopbackFile) Chown(uid uint32, gid uint32) fuse.Status {
	return fuse.EPERM
}

func (me *LazyLoopbackFile) Chmod(perms uint32) fuse.Status {
	return fuse.EPERM
}
