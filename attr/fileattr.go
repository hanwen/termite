package attr

import (
	"crypto"
	"fmt"
	"github.com/hanwen/go-fuse/fuse"
	"io/ioutil"
	"log"
	"os"
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
				m := fuse.FileMode(fuse.ToAttr(v).Mode &^ 07777)
				if m != 0 {
					// m == 0 may happen for fuse mounts that have died.
					me.NameModeMap[v.Name()] = m
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
