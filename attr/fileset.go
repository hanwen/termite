package attr

import (
	"fmt"
	"sort"
)

type FileSet struct {
	Files []*FileAttr
}

func (me *FileSet) String() string {
	return fmt.Sprintf("%v", me.Files)
}

func (me *FileSet) Len() int {
	return len(me.Files)
}

func (me *FileSet) Less(i, j int) bool {
	a := me.Files[i]
	b := me.Files[j]
	if a.Deletion() != b.Deletion() {
		return a.Deletion()
	}

	if a.Deletion() {
		return a.Path > b.Path
	}
	return a.Path < b.Path
}

func (me *FileSet) Swap(i, j int) {
	me.Files[i], me.Files[j] = me.Files[j], me.Files[i]
}

func (me *FileSet) Sort() {
	sort.Sort(me)
}
