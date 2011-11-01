package termite

import (
	"fmt"
	"github.com/hanwen/go-fuse/fuse"
	"io/ioutil"
	"log"
	"os"
	"syscall"
)

var _ = log.Printf

func (me FileAttr) String() string {
	id := me.Path

	if me.Hash != "" {
		id += fmt.Sprintf(" sz %d md5 %x..", me.FileInfo.Size, me.Hash[:4])
	}
	if me.Link != "" {
		id += fmt.Sprintf(" -> %s", me.Link)
	}
	if me.FileInfo != nil {
		id += fmt.Sprintf(" %s:%o", FileMode(me.FileInfo.Mode), me.FileInfo.Mode&07777)
		if me.NameModeMap != nil {
			id += "+names"
		}
	} else {
		id += " (del)"
	}
	return id
}

func (me FileAttr) LongString() string {
	s := me.String()
	if me.FileInfo != nil {
		s += fmt.Sprintf(" C%d.%09d, M%d.%09d, A%d.%09d",
			me.Ctime_ns/1e9,
			me.Ctime_ns%1e9,
			me.Mtime_ns/1e9,
			me.Mtime_ns%1e9,
			me.Atime_ns/1e9,
			me.Atime_ns%1e9)
	}
	return s
}

func (me FileAttr) Deletion() bool {
	return me.FileInfo == nil
}

func (me FileAttr) Status() fuse.Status {
	if me.Deletion() {
		return fuse.ENOENT
	}
	return fuse.OK
}

func (me FileAttr) Copy(withdir bool) *FileAttr {
	a := me
	if me.NameModeMap != nil && withdir {
		a.NameModeMap = map[string]FileMode{}
		for k, v := range me.NameModeMap {
			a.NameModeMap[k] = v
		}
	} else {
		a.NameModeMap = nil
	}
	return &a
}

func (me *FileAttr) ReadFromFs(p string) {
	var err os.Error
	switch {
	case me.IsRegular():
		if c, e := ioutil.ReadFile(p); e == nil {
			me.Hash = md5(c)
		} else {
			err = e
		}

	case me.IsDirectory():
		d, e := ioutil.ReadDir(p)
		if e == nil {
			me.NameModeMap = make(map[string]FileMode, len(d))
			for _, v := range d {
				m := FileMode(v.Mode &^ 07777)
				if m != 0 {
					// m == 0 may happen for fuse mounts that have died.
					me.NameModeMap[v.Name] = m
				}
			}
		} else {
			err = e
		}

	case me.IsSymlink():
		if l, e := os.Readlink(p); e == nil {
			me.Link = l
		} else {
			err = e
		}
	}

	if err != nil {
		log.Panicf("Error reading %q (%s): %v", FileMode(me.FileInfo.Mode), p, err)
		me.FileInfo = nil
	}
}

func (me *FileAttr) Merge(r FileAttr) {
	if r.Path != me.Path {
		panic("path mismatch")
	}

	if r.Deletion() {
		panic("should not merge deletions")
	}

	other := r.NameModeMap
	mine := me.NameModeMap
	*me = r
	me.NameModeMap = nil

	if me.IsDirectory() {
		if other != nil {
			me.NameModeMap = make(map[string]FileMode)
			for k, v := range other {
				me.NameModeMap[k] = v
			}
		} else {
			me.NameModeMap = mine
		}
	}
}

// cut & paste from os/types.go

func (me FileMode) String() string {
	switch uint32(me) & syscall.S_IFMT {
	case syscall.S_IFIFO:
		return "p"
	case syscall.S_IFCHR:
		return "c"
	case syscall.S_IFDIR:
		return "d"
	case syscall.S_IFBLK:
		return "b"
	case syscall.S_IFREG:
		return "f"
	case syscall.S_IFLNK:
		return "l"
	case syscall.S_IFSOCK:
		return "s"
	default:
		log.Panic("Unknown mode: %o", me)
	}
	return "0"
}

func (me FileMode) IsFifo() bool { return (uint32(me) & syscall.S_IFMT) == syscall.S_IFIFO }

// IsChar reports whether the FileInfo describes a character special file.
func (me FileMode) IsChar() bool { return (uint32(me) & syscall.S_IFMT) == syscall.S_IFCHR }

// IsDirectory reports whether the FileInfo describes a directory.
func (me FileMode) IsDirectory() bool { return (uint32(me) & syscall.S_IFMT) == syscall.S_IFDIR }

// IsBlock reports whether the FileInfo describes a block special file.
func (me FileMode) IsBlock() bool { return (uint32(me) & syscall.S_IFMT) == syscall.S_IFBLK }

// IsRegular reports whether the FileInfo describes a regular file.
func (me FileMode) IsRegular() bool { return (uint32(me) & syscall.S_IFMT) == syscall.S_IFREG }

// IsSymlink reports whether the FileInfo describes a symbolic link.
func (me FileMode) IsSymlink() bool { return (uint32(me) & syscall.S_IFMT) == syscall.S_IFLNK }

// IsSocket reports whether the FileInfo describes a socket.
func (me FileMode) IsSocket() bool { return (uint32(me) & syscall.S_IFMT) == syscall.S_IFSOCK }
