package termite

import (
	"github.com/hanwen/go-fuse/fuse"
	"os"
	"syscall"
)

const _NULL = "null"

// Expose one file "null" with /dev/null behavior.
type DevNullFs struct {
	fuse.DefaultFileSystem
}

func (me *DevNullFs) GetAttr(name string) (*os.FileInfo, fuse.Status) {
	if name == "" {
		fi := os.FileInfo{Mode: fuse.S_IFDIR | 0777}
		return &fi, fuse.OK
	}
	if name == _NULL {
		fi := os.FileInfo{Mode: fuse.S_IFREG | 0666}
		return &fi, fuse.OK
	}

	return nil, fuse.ENOSYS
}

func (me *DevNullFs) OpenDir(name string) (stream chan fuse.DirEntry, status fuse.Status) {
	if name == "" {
		stream := make(chan fuse.DirEntry, 2)
		stream <- fuse.DirEntry{fuse.S_IFREG | 0666, _NULL}
		close(stream)
		return stream, fuse.OK
	}

	return nil, fuse.ENOENT
}

func (me *DevNullFs) GetXAttr(name string, attr string) ([]byte, fuse.Status) {
	return nil, syscall.ENODATA
}

func (me *DevNullFs) Open(name string, flags uint32) (file fuse.File, code fuse.Status) {
	if name == _NULL {
		return fuse.NewDevNullFile(), fuse.OK
	}

	return nil, fuse.ENOENT
}

func (me *DevNullFs) Access(name string, mode uint32) (code fuse.Status) {
	if name == _NULL {
		return fuse.OK
	}

	return fuse.ENOENT
}

func (me *DevNullFs) Truncate(name string, offset uint64) (code fuse.Status) {
	if name == _NULL {
		return fuse.OK
	}
	return fuse.ENOENT
}
