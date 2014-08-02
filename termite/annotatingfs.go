package termite

import (
	"github.com/hanwen/go-fuse/fuse"
	"github.com/hanwen/go-fuse/fuse/nodefs"
	"github.com/hanwen/go-fuse/fuse/pathfs"
	"sync"
)

type AnnotatingFS struct {
	pathfs.FileSystem

	openedMu sync.Mutex
	opened   map[string]struct{}
}

func NewAnnotatingFS(fs pathfs.FileSystem) *AnnotatingFS {
	return &AnnotatingFS{
		opened:     map[string]struct{}{},
		FileSystem: fs,
	}
}

func (fs *AnnotatingFS) Open(name string, flags uint32, context *fuse.Context) (file nodefs.File, code fuse.Status) {
	f, code := fs.FileSystem.Open(name, flags, context)
	if code.Ok() {
		fs.openedMu.Lock()
		fs.opened[name] = struct{}{}
		fs.openedMu.Unlock()
	}
	return f, code
}

func (fs *AnnotatingFS) Reap() []string {
	fs.openedMu.Lock()
	r := make([]string, 0, len(fs.opened))
	for k := range fs.opened {
		r = append(r, k)
	}
	fs.opened = map[string]struct{}{}
	fs.openedMu.Unlock()
	return r
}
