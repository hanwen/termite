package attr

import (
	"crypto"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"syscall"

	"github.com/hanwen/go-fuse/fuse"
)

type FileMode uint32

func (m FileMode) String() string {
	nm := "?"
	switch int(m &^ 07777) {
	case syscall.S_IFDIR:
		nm = "d"
	case syscall.S_IFREG:
		nm = "f"
	case syscall.S_IFLNK:
		nm = "l"
	case syscall.S_IFSOCK:
		nm = "s"
	default:
		nm = fmt.Sprintf("%x", uint32(m)^07777)
	}

	return fmt.Sprintf("%s:%o", nm, uint32(m&07777))
}

type FileAttr struct {
	// Full path of the file
	Path string

	// Attr holds the FUSE attributes
	*fuse.Attr

	// Hash holds the cryptographic has of the file, in case of a
	// normal file.
	Hash string

	// Link holds the link target in case of a symlink.
	Link string

	// Only filled for directories.
	NameModeMap map[string]FileMode
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
		id += FileMode(me.Attr.Mode).String()
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
		a.NameModeMap = make(map[string]FileMode, len(me.NameModeMap))
		for k, v := range me.NameModeMap {
			a.NameModeMap[k] = v
		}
	} else {
		a.NameModeMap = nil
	}
	return &a
}

const _TERM_XATTR = "user.termattr"

// EncodedAttr is the key that we use for file content equality. It is
// smaller than syscall.Stat_t and similar structures so it can be
// efficiently stored as extended attribute.
type EncodedAttr struct {
	Size    uint64
	Mtimens uint64
	Perm    uint16
	Nlink   uint16
}

const sizeEncodedAttr = 20

func (e *EncodedAttr) FromAttr(a *fuse.Attr) {
	e.Perm = uint16(a.Mode & 07777)
	e.Nlink = uint16(a.Nlink)
	e.Size = a.Size
	e.Mtimens = 1e9*a.Mtime + uint64(a.Mtimensec)
}

func (e *EncodedAttr) Eq(b *EncodedAttr) bool {
	return e.Perm == b.Perm &&
		e.Nlink == b.Nlink &&
		e.Size == b.Size &&
		e.Mtimens == b.Mtimens
}

func (e *EncodedAttr) Encode(h string) []byte {
	a := []byte{
		byte(e.Perm),
		byte(e.Perm >> 8),
		byte(e.Nlink),
		byte(e.Nlink >> 8),
		byte(e.Size),
		byte(e.Size >> 8),
		byte(e.Size >> 16),
		byte(e.Size >> 24),
		byte(e.Size >> 32),
		byte(e.Size >> 40),
		byte(e.Size >> 48),
		byte(e.Size >> 56),
		byte(e.Mtimens),
		byte(e.Mtimens >> 8),
		byte(e.Mtimens >> 16),
		byte(e.Mtimens >> 24),
		byte(e.Mtimens >> 32),
		byte(e.Mtimens >> 40),
		byte(e.Mtimens >> 48),
		byte(e.Mtimens >> 56),
	}

	b := make([]byte, len(a)+len(h))
	copy(b, a)
	copy(b[len(a):], h)
	return b
}

func (e *EncodedAttr) Decode(in []byte) (hash []byte) {
	if uintptr(len(in)) < sizeEncodedAttr {
		return nil
	}
	e.Perm = uint16(in[0]) | uint16(in[1])<<8
	in = in[2:]
	e.Nlink = uint16(in[0]) | uint16(in[1])<<8
	in = in[2:]
	e.Size = uint64(in[0]) | uint64(in[1])<<8 | uint64(in[2])<<16 | uint64(in[3])<<24 | uint64(in[4])<<32 | uint64(in[5])<<40 | uint64(in[6])<<48 | uint64(in[7])<<56
	in = in[8:]
	e.Mtimens = uint64(in[0]) | uint64(in[1])<<8 | uint64(in[2])<<16 | uint64(in[3])<<24 | uint64(in[4])<<32 | uint64(in[5])<<40 | uint64(in[6])<<48 | uint64(in[7])<<56
	in = in[8:]

	return in
}

func (a *FileAttr) WriteXAttr(p string) {
	if a.Attr == nil {
		return
	}

	var e EncodedAttr
	e.FromAttr(a.Attr)

	b := e.Encode(a.Hash)
	errno := syscall.Setxattr(p, _TERM_XATTR, b, 0)
	if errno != nil {
		log.Printf("Setxattr %s: code %v", p, errno)
	}
}

func (e *EncodedAttr) ReadXAttr(path string) (hash []byte) {
	b := make([]byte, 64)
	val, errno := syscall.Getxattr(path, _TERM_XATTR, b)
	if errno == nil {
		// TODO needs test.
		return e.Decode(b[:val])
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
			me.NameModeMap = make(map[string]FileMode, len(d))
			for _, v := range d {
				a := fuse.ToAttr(v)
				if a != nil {
					// attr == nil may happen for fuse mounts that have died.
					me.NameModeMap[v.Name()] = FileMode(a.Mode &^ 07777)
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
			me.NameModeMap = make(map[string]FileMode)
			for k, v := range other {
				me.NameModeMap[k] = v
			}
		} else {
			me.NameModeMap = mine
		}
	}
}

func FuseAttrEq(a *fuse.Attr, b *fuse.Attr) bool {
	return (a.Mode == b.Mode && a.Size == b.Size && a.Mtime == b.Mtime &&
		a.Nlink == b.Nlink &&
		a.Ino == b.Ino && a.Mtimensec == b.Mtimensec && a.Uid == b.Uid &&
		a.Gid == b.Gid)
}

func (me *FileAttr) IsFifo() bool {
	return me.Attr != nil && (uint32(me.Mode)&syscall.S_IFMT) == syscall.S_IFIFO
}

// IsChar reports whether the FileInfo describes a character special file.
func (me *FileAttr) IsChar() bool {
	return me.Attr != nil && (uint32(me.Mode)&syscall.S_IFMT) == syscall.S_IFCHR
}

// IsDir reports whether the FileInfo describes a directory.
func (me *FileAttr) IsDir() bool {
	return me.Attr != nil && (uint32(me.Mode)&syscall.S_IFMT) == syscall.S_IFDIR
}

// IsBlock reports whether the FileInfo describes a block special file.
func (me *FileAttr) IsBlock() bool {
	return me.Attr != nil && (uint32(me.Mode)&syscall.S_IFMT) == syscall.S_IFBLK
}

// IsRegular reports whether the FileInfo describes a regular file.
func (me *FileAttr) IsRegular() bool {
	return me.Attr != nil && (uint32(me.Mode)&syscall.S_IFMT) == syscall.S_IFREG
}

// IsSymlink reports whether the FileInfo describes a symbolic link.
func (me *FileAttr) IsSymlink() bool {
	return me.Attr != nil && (uint32(me.Mode)&syscall.S_IFMT) == syscall.S_IFLNK
}

// IsSocket reports whether the FileInfo describes a socket.
func (me *FileAttr) IsSocket() bool {
	return me.Attr != nil && (uint32(me.Mode)&syscall.S_IFMT) == syscall.S_IFSOCK
}
