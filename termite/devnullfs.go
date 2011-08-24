package termite

import (
	"fmt"
	"github.com/hanwen/go-fuse/fuse"
	"os"
	"syscall"
)

const _NULL = "null"

// Expose one entry in a directory.
type EntryFs struct {
	fuse.DefaultFileSystem
	os.FileInfo
}

func NewEntryFs(fi os.FileInfo) *EntryFs {
	e := EntryFs{}
	e.FileInfo = fi
	return &e
}

func (me *EntryFs) Name() string {
	return fmt.Sprintf("EntryFs(%s)", me.FileInfo.Name)
}

func (me *EntryFs) GetAttr(name string, context *fuse.Context) (*os.FileInfo, fuse.Status) {
	if name == "" {
		fi := os.FileInfo{Mode: fuse.S_IFDIR | 0777}
		return &fi, fuse.OK
	}

	if name == me.FileInfo.Name {
		fi := me.FileInfo
		return &fi, fuse.OK
	}

	return nil, fuse.ENOENT
}

func (me *EntryFs) OpenDir(name string, context *fuse.Context) (stream chan fuse.DirEntry, status fuse.Status) {
	if name == "" {
		stream := make(chan fuse.DirEntry, 2)
		stream <- fuse.DirEntry{fuse.S_IFREG | 0666, me.FileInfo.Name}
		close(stream)
		return stream, fuse.OK
	}

	return nil, fuse.ENOENT
}

func (me *EntryFs) GetXAttr(name string, attr string, context *fuse.Context) ([]byte, fuse.Status) {
	return nil, syscall.ENODATA
}

func (me *EntryFs) Access(name string, mode uint32, context *fuse.Context) (code fuse.Status) {
	if name == me.FileInfo.Name {
		return fuse.OK
	}

	return fuse.ENOENT
}


// DevnullFs: a single entry, the 'null' file.

type DevnullFs struct {
	EntryFs
}

func NewDevnullFs() *DevnullFs {
	return &DevnullFs{
		EntryFs: EntryFs{
		FileInfo: os.FileInfo{ Mode: fuse.S_IFREG | 0644, Name: "null"},
		},
	}
}

func (me *DevnullFs) Name() string {
	return fmt.Sprintf("DevnullFs")
}

func (me *DevnullFs) Truncate(name string, offset uint64, context *fuse.Context) (code fuse.Status) {
	if name == me.FileInfo.Name {
		return fuse.OK
	}
	return fuse.ENOENT
}

func (me *EntryFs) Open(name string, flags uint32, context *fuse.Context) (file fuse.File, code fuse.Status) {
	if name == me.FileInfo.Name {
		return fuse.NewDevNullFile(), fuse.OK
	}

	return nil, fuse.ENOENT
}


