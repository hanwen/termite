package termite

import (
	"fmt"
	"github.com/hanwen/go-fuse/fuse"
	"log"
	"os"
	"sync"
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

func (me *LazyLoopbackFile) Read(r *fuse.ReadIn, bp fuse.BufferPool) ([]byte, fuse.Status) {
	f, s := me.file()
	if s.Ok() {
		return f.Read(r, bp)
	}
	return nil, s
}

func (me *LazyLoopbackFile) Release() {
	me.mu.Lock()
	defer me.mu.Unlock()
	if me.f != nil {
		me.f.Release()
	}
}

func (me *LazyLoopbackFile) Write(w *fuse.WriteIn, s []byte) (uint32, fuse.Status) {
	return 0, fuse.EPERM
}

func (me *LazyLoopbackFile) GetAttr() (*os.FileInfo, fuse.Status) {
	f, s := me.file()
	if s.Ok() {
		return f.GetAttr()
	}
	return nil, s
}

func (me *LazyLoopbackFile) Utimens(atimeNs uint64, mtimeNs uint64) fuse.Status {
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
