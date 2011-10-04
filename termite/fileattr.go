package termite

import (
	"github.com/hanwen/go-fuse/fuse"
)

func (me FileAttr) Deletion() bool {
	return me.FileInfo == nil
}

func (me FileAttr) Status() fuse.Status {
	if me.Deletion() {
		return fuse.ENOENT
	}
	return fuse.OK
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
