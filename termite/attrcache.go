package termite

import (
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

type AttributeCache struct {
	mutex sync.RWMutex
	attributes      map[string]*FileAttr
	cond  *sync.Cond
	busy  map[string]bool
	getter         func(name string)*FileAttr
	statter        func(name string)*os.FileInfo
}

func NewAttributeCache(getter func(n string)*FileAttr,
	statter func(n string)*os.FileInfo) *AttributeCache {
	me := &AttributeCache{
		attributes: make(map[string]*FileAttr),
		busy:  map[string]bool{},
	}
	me.cond = sync.NewCond(&me.mutex)
	me.getter = getter
	me.statter = statter
	return me
}

var paranoia = false

func (me *AttributeCache) verify() {
	if !paranoia {
		return
	}
	me.mutex.RLock()
	defer me.mutex.RUnlock()

	for k, v := range me.attributes {
		if v.Path != k {
			log.Panicf("attributes mismatch %q %#v", k, v)
		}
		if _, ok := me.busy[k]; ok {
			log.Panicf("busy and attributes entry for %q", k)
		}
		if v.Deletion() {
			log.Panicf("Attribute cache may not contain deletions %q", k)
		}

		dir, base := filepath.Split(k)
		dir = strings.TrimRight(dir, string(filepath.Separator))
		if base != k {
			parent := me.attributes[dir]
			if v.Deletion() && parent != nil && parent.NameModeMap[base] != 0 {
				log.Panicf("Parent %q has entry for deleted %q", dir, base)
			}
			if !v.Deletion() && parent == nil {
				log.Panicf("Missing parent for %q", k)
			}
			if !v.Deletion() && parent.NameModeMap[base] == 0 {
				log.Panicf("Parent %q has no entry for %q", dir, base)
			}
		}
	}
}

func (me *AttributeCache) Have(name string) bool {
	me.mutex.RLock()
	defer me.mutex.RUnlock()
	_, ok := me.attributes[name]
	return ok
}
	
func (me *AttributeCache) Get(name string) (rep *FileAttr) {
	me.mutex.RLock()
	rep, ok := me.attributes[name]
	me.mutex.RUnlock()
	if ok {
		return rep
	}

	defer me.verify()
	me.mutex.Lock()
	defer me.mutex.Unlock()
	for me.busy[name] && me.attributes[name] == nil {
		me.cond.Wait()
	}
	rep, ok = me.attributes[name]
	if ok {
		return rep
	}
	me.busy[name] = true
	me.mutex.Unlock()

	rep = me.getter(name)
	me.mutex.Lock()
	if !rep.Deletion() {
		me.attributes[name] = rep
	}
	me.cond.Broadcast()
	me.busy[name] = false, false
	return rep
}

func (me *AttributeCache) Update(files []*FileAttr) {
	defer me.verify()
	me.mutex.Lock()
	defer me.mutex.Unlock()
	me.update(files)
}

func (me *AttributeCache) update(files []*FileAttr) {
	attributes := me.attributes
	for _, inF := range files {
		r := *inF
		if len(r.Path) > 0 && r.Path[0] == '/' {
			panic("Leading slash.")
		}

		dir, basename := filepath.Split(r.Path)
		dir = strings.TrimRight(dir, string(filepath.Separator))
		dirAttr := attributes[dir]
		if dirAttr == nil {
			log.Panicf("missing parent dir: %q", dir)
		}
		if dirAttr.NameModeMap == nil {
			log.Panicf("parent dir has no NameModeMap: %q", dir)
		}
		if r.Deletion() {
			dirAttr.NameModeMap[basename] = 0, false
		} else {
			dirAttr.NameModeMap[basename] = r.Mode &^ 0777
		}

		if r.Deletion() {
			attributes[r.Path] = nil, false
			continue
		}

		old := attributes[r.Path]
		if old == nil {
			old = &r
			attributes[r.Path] = old
		}
		old.Merge(r)
	}
}

func (me *AttributeCache) Refresh(prefix string) FileSet {
	defer me.verify()
	me.mutex.Lock()
	defer me.mutex.Unlock()

	if prefix != "" && prefix[0] == '/' {
		panic("leading /")
	}

	updated := []*FileAttr{}
	for key, attr := range me.attributes {
		// TODO -should just do everything?
		if !strings.HasPrefix(key, prefix) {
			continue
		}

		fi := me.statter(key)
		if fi == nil && !attr.Deletion() {
			del := FileAttr{
				Path: key,
			}
			updated = append(updated, &del)
		}
		// TODO - does this handle symlinks corrrectly?
		if fi != nil && attr.FileInfo != nil && EncodeFileInfo(*attr.FileInfo) != EncodeFileInfo(*fi) {
			newEnt := me.getter(key)
			updated = append(updated, newEnt)
		}
	}

	fs := FileSet{updated}
	fs.Sort()
	me.update(fs.Files)
	return fs
}

func (me *AttributeCache) Copy() FileSet {
	me.mutex.RLock()
	defer me.mutex.RUnlock()

	dump := []*FileAttr{}
	for _, attr := range me.attributes {
		copy := *attr
		dump = append(dump, &copy)
	}

	fs := FileSet{dump}
	fs.Sort()
	return fs
}
