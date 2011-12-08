package attr

import (
	"crypto"
	"fmt"
	"github.com/hanwen/go-fuse/fuse"
	"io/ioutil"
	"log"
	"os"
	"syscall"
)

var _ = log.Printf

type FileAttr struct {
	Path string
	*fuse.Attr
	Hash string
	Link string

	// Only filled for directories.
	NameModeMap map[string]fuse.FileMode
}

func (me FileAttr) String() string {
	id := me.Path

	if me.Hash != "" {
		id += fmt.Sprintf(" sz %d md5 %x..", me.Attr.Size, me.Hash[:4])
	}
	if me.Link != "" {
		id += fmt.Sprintf(" -> %s", me.Link)
	}
	if me.Attr != nil {
		id += fmt.Sprintf(" %s:%o", fuse.FileMode(me.Attr.Mode), me.Attr.Mode&07777)
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
	if me.Attr != nil {
		s += fmt.Sprintf(" %v", me.Attr)
	}
	return s
}

func (me FileAttr) Deletion() bool {
	return me.Attr == nil
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
		a.NameModeMap = map[string]fuse.FileMode{}
		for k, v := range me.NameModeMap {
			a.NameModeMap[k] = v
		}
	} else {
		a.NameModeMap = nil
	}
	return &a
}

const _TERM_XATTR = "user.termattr"

func (a *FileAttr) WriteXAttr(p string) {
	b, err := a.Encode()
	if err != nil {
		log.Panic("Encode", a, err)
	}
	errno := fuse.Setxattr(p, _TERM_XATTR, b, 0)
	if errno != 0 {
		log.Printf("Setxattr %s: code %v", p, syscall.Errno(errno))
	}
}
	
func ReadXAttr(path string) *FileAttr {
	val, errno := fuse.GetXAttr(path, _TERM_XATTR)
	if errno == 0 {
		read := FileAttr{}
		err := read.Decode(val)
		if err == nil {
			return &read
		}
	}
	return nil
}

func (me *FileAttr) ReadFromFs(p string, hashFunc crypto.Hash) {
	var err error
	switch {
	case me.IsRegular():
		if c, e := ioutil.ReadFile(p); e == nil {
			h := hashFunc.New()
			h.Write(c)
			me.Hash = string(h.Sum(nil))
		} else {
			err = e
		}

	case me.IsDir():
		d, e := ioutil.ReadDir(p)
		if e == nil {
			me.NameModeMap = make(map[string]fuse.FileMode, len(d))
			for _, v := range d {
				a := fuse.ToAttr(v)
				if a != nil {
					// attr == nil may happen for fuse mounts that have died.
					me.NameModeMap[v.Name()] = fuse.FileMode(a.Mode &^ 07777)
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
		log.Println("Error reading %q (%s): %v", me.Attr.Mode, p, err)
		me.Attr = nil
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

	if me.IsDir() {
		if other != nil {
			me.NameModeMap = make(map[string]fuse.FileMode)
			for k, v := range other {
				me.NameModeMap[k] = v
			}
		} else {
			me.NameModeMap = mine
		}
	}
}

func FuseAttrEq(a *fuse.Attr, b *fuse.Attr) bool {
	return (a.Mode == b.Mode && a.Size == b.Size && a.Blocks == b.Blocks && a.Mtime == b.Mtime && a.Ctime == b.Ctime &&
		a.Mtimensec == b.Mtimensec && a.Ctimensec == b.Ctimensec && a.Uid == b.Uid && a.Gid == b.Gid && a.Blksize == b.Blksize)
}

// Writing the attr sets the Ctime.
func FuseAttrEqNoCtime(a *fuse.Attr, b *fuse.Attr) bool {
	return (a.Mode == b.Mode && a.Size == b.Size && a.Blocks == b.Blocks && a.Mtime == b.Mtime && 
		a.Mtimensec == b.Mtimensec && a.Uid == b.Uid && a.Gid == b.Gid && a.Blksize == b.Blksize)
}

func (me *FileAttr) IsFifo() bool { return me.Attr != nil && (uint32(me.Mode) & syscall.S_IFMT) == syscall.S_IFIFO }

// IsChar reports whether the FileInfo describes a character special file.
func (me *FileAttr) IsChar() bool { return me.Attr != nil &&  (uint32(me.Mode) & syscall.S_IFMT) == syscall.S_IFCHR }

// IsDir reports whether the FileInfo describes a directory.
func (me *FileAttr) IsDir() bool { return me.Attr != nil &&  (uint32(me.Mode) & syscall.S_IFMT) == syscall.S_IFDIR }

// IsBlock reports whether the FileInfo describes a block special file.
func (me *FileAttr) IsBlock() bool { return me.Attr != nil &&  (uint32(me.Mode) & syscall.S_IFMT) == syscall.S_IFBLK }

// IsRegular reports whether the FileInfo describes a regular file.
func (me *FileAttr) IsRegular() bool { return me.Attr != nil &&  (uint32(me.Mode) & syscall.S_IFMT) == syscall.S_IFREG }

// IsSymlink reports whether the FileInfo describes a symbolic link.
func (me *FileAttr) IsSymlink() bool { return me.Attr != nil &&  (uint32(me.Mode) & syscall.S_IFMT) == syscall.S_IFLNK }

// IsSocket reports whether the FileInfo describes a socket.
func (me *FileAttr) IsSocket() bool { return me.Attr != nil &&  (uint32(me.Mode) & syscall.S_IFMT) == syscall.S_IFSOCK }

