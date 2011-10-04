package termite

import (
	"github.com/hanwen/go-fuse/fuse"
	"io/ioutil"
	"log"
	"os"
)

var _ = log.Printf

func (me FileAttr) Deletion() bool {
	return me.FileInfo == nil
}

func (me FileAttr) Status() fuse.Status {
	if me.Deletion() {
		return fuse.ENOENT
	}
	return fuse.OK
}

func (me FileAttr) Copy() *FileAttr {
	a := me
	if me.NameModeMap != nil {
		a.NameModeMap = map[string]uint32{}
		for k, v := range me.NameModeMap {
			a.NameModeMap[k] = v
		}
	}
	return &a
}

func (me *FileAttr) ReadFromFs(p string) {
	switch {
	case me.IsRegular():
		c, e := ioutil.ReadFile(p)
		if e == nil {
			me.Hash = md5(c)
		} else {
			me.FileInfo = nil
		}
	case me.IsDirectory():
		d, e := ioutil.ReadDir(p)
		if e == nil {
			me.NameModeMap = make(map[string]uint32, len(d))
			for _, v := range d {
				me.NameModeMap[v.Name] = v.Mode
			}
		} else {
			me.FileInfo = nil
		}
	case me.IsSymlink():
		l, e := os.Readlink(p)
		if e == nil {
			me.Link = l
		} else {
			me.FileInfo = nil
		}
	}
}

func (me *FileAttr) Merge(r FileAttr) {
	if r.Path != me.Path {
		panic("path mismatch")
	}

	if r.Deletion() {
		panic("should not merge deletions")
	}
	
	mine := me.NameModeMap
	other := r.NameModeMap

	*me = r
	me.NameModeMap = nil

	if me.FileInfo.IsDirectory() {
		me.NameModeMap = mine
		if me.NameModeMap == nil {
			me.NameModeMap = make(map[string]uint32)
		}
		for k, v := range other {
			me.NameModeMap[k] = v
		}
	}
}
