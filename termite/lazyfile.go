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

func (f *lazyLoopbackFile) file() (nodefs.File, fuse.Status) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.f == nil {
		file, err := os.Open(f.Name)
		if err != nil {
			return nil, fuse.ToStatus(err)
		}
		f.f = nodefs.NewLoopbackFile(file)
	}
	return f.f, fuse.OK
}

func (f *lazyLoopbackFile) InnerFile() nodefs.File {
	inner, _ := f.file()
	return inner
}

func (f *lazyLoopbackFile) String() string {
	return fmt.Sprintf("lazyLoopbackFile(%s)", f.Name)
}

func (f *lazyLoopbackFile) Read(buf []byte, off int64) (fuse.ReadResult, fuse.Status) {
	inner, s := f.file()
	if s.Ok() {
		return inner.Read(buf, off)
	}
	return nil, fuse.OK
}

func (f *lazyLoopbackFile) Release() {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.f != nil {
		f.f.Release()
	}
}

func (f *lazyLoopbackFile) Write(s []byte, off int64) (uint32, fuse.Status) {
	return 0, fuse.EPERM
}

func (f *lazyLoopbackFile) GetAttr(a *fuse.Attr) fuse.Status {
	inner, s := f.file()
	if s.Ok() {
		return inner.GetAttr(a)
	}
	return s
}

func (f *lazyLoopbackFile) Utimens(atimeNs, mtimeNs *time.Time) fuse.Status {
	return fuse.EPERM
}

func (f *lazyLoopbackFile) Truncate(size uint64) fuse.Status {
	return fuse.EPERM
}

func (f *lazyLoopbackFile) Chown(uid uint32, gid uint32) fuse.Status {
	return fuse.EPERM
}

func (f *lazyLoopbackFile) Chmod(perms uint32) fuse.Status {
	return fuse.EPERM
}
