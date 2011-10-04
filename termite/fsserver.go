package termite

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

var _ = fmt.Println

type FsServer struct {
	contentServer  *ContentServer
	contentCache   *ContentCache
	Root           string
	excluded       map[string]bool
	excludePrivate bool

	hashCacheMutex sync.RWMutex
	hashCacheCond  *sync.Cond
	hashCache      map[string]string
	hashBusyMap    map[string]bool

	attrCacheMutex sync.RWMutex
	attrCache      map[string]*FileAttr
	attrCacheCond  *sync.Cond
	attrCacheBusy  map[string]bool

	// TODO - add counters and check that the rpcFs.fetchCond is
	// working.
}

var paranoia = false

func (me *FsServer) verify() {
	if !paranoia {
		return
	}
	me.attrCacheMutex.RLock()
	defer me.attrCacheMutex.RUnlock()

	for k, v := range me.attrCache {
		if v.Path != k {
			log.Panicf("attrCache mismatch %q %#v", k, v)
		}
		if _, ok := me.attrCacheBusy[k]; ok {
			log.Panicf("attrCacheBusy and attrCache entry for %q", k)
		}
	}
}

func NewFsServer(root string, cache *ContentCache, excluded []string) *FsServer {
	fs := &FsServer{
		contentCache:  cache,
		contentServer: &ContentServer{Cache: cache},
		Root:          root,
		hashCache:     make(map[string]string),
		hashBusyMap:   map[string]bool{},

		attrCache:      make(map[string]*FileAttr),
		attrCacheBusy:  map[string]bool{},
		excludePrivate: true,
	}

	fs.hashCacheCond = sync.NewCond(&fs.hashCacheMutex)
	fs.attrCacheCond = sync.NewCond(&fs.attrCacheMutex)
	fs.excluded = make(map[string]bool)
	for _, e := range excluded {
		if e[0] == '/' {
			panic("leading slash")
		}
		fs.excluded[e] = true
	}
	return fs
}

func (me *FsServer) path(n string) string {
	if me.Root == "" {
		return n
	}
	return filepath.Join(me.Root, strings.TrimLeft(n, "/"))
}

func (me *FsServer) FileContent(req *ContentRequest, rep *ContentResponse) os.Error {
	return me.contentServer.FileContent(req, rep)
}

func (me *FsServer) GetAttr(req *AttrRequest, rep *AttrResponse) os.Error {
	log.Printf("GetAttr req %q", req.Name)
	if req.Name != "" && req.Name[0] == '/' {
		panic("leading /")
	}

	a := me.oneGetAttr(req.Name)
	if a.Hash != "" {
		log.Printf("GetAttr %v %x", a, a.Hash)
	}
	rep.Attrs = append(rep.Attrs, a)
	return nil
}

func (me *FsServer) uncachedGetAttr(name string) (rep *FileAttr) {
	if me.excluded[name] {
		log.Printf("Denied access to excluded file %q", name)
		return &FileAttr{
			Path: name,
		}
	}
	p := me.path(name)
	fi, _ := os.Lstat(p)
	rep = &FileAttr{
		FileInfo: fi,
		Path:     name,
	}

	// We don't want to expose the master's private files to the
	// world.
	if me.excludePrivate && fi != nil && fi.Mode&0077 == 0 {
		log.Printf("Denied access to private file %q", name)
		rep.FileInfo = nil
		fi = nil
	}

	if fi != nil {
		me.fillContent(rep)
	}
	return rep
}

func (me *FsServer) oneGetAttr(name string) (rep *FileAttr) {
	defer me.verify()
	me.attrCacheMutex.RLock()
	rep, ok := me.attrCache[name]
	me.attrCacheMutex.RUnlock()
	if ok {
		return rep
	}

	me.attrCacheMutex.Lock()
	defer me.attrCacheMutex.Unlock()
	for me.attrCacheBusy[name] && me.attrCache[name] == nil {
		me.attrCacheCond.Wait()
	}
	rep, ok = me.attrCache[name]
	if ok {
		return rep
	}
	me.attrCacheBusy[name] = true
	me.attrCacheMutex.Unlock()

	rep = me.uncachedGetAttr(name)

	me.attrCacheMutex.Lock()
	me.attrCache[name] = rep
	me.attrCacheCond.Broadcast()
	me.attrCacheBusy[name] = false, false
	return rep
}

func (me *FsServer) fillContent(rep *FileAttr) {
	if rep.FileInfo.IsSymlink() {
		rep.Link, _ = os.Readlink(me.path(rep.Path))
	}
	if rep.FileInfo.IsRegular() {
		rep.Hash = me.getHash(rep.Path)
		if rep.Hash == "" {
			// Typically happens if we want to open /etc/shadow as normal user.
			log.Println("fillContent returning EPERM for", rep.Path)
			rep.FileInfo = nil
		}
	}
	if rep.FileInfo.IsDirectory() {
		p := me.path(rep.Path)
		d, e := ioutil.ReadDir(p)
		if e == nil {
			rep.NameModeMap = make(map[string]uint32)
			for _, v := range d {
				rep.NameModeMap[v.Name] = v.Mode
			}
		} else {
			rep.FileInfo = nil
		}
	}
}

func (me *FsServer) updateFiles(infos []*FileAttr) {
	me.updateHashes(infos)
	me.updateAttrs(infos)
}

func updateAttributeMap(attributes map[string]*FileAttr, files []*FileAttr) {
	log.Println(files)
	for _, inF := range files {
		r := *inF
		if len(r.Path) > 0 && r.Path[0] == '/' {
			panic("Leading slash.")
		}

		dir, basename := filepath.Split(r.Path)
		dir = strings.TrimRight(dir, string(filepath.Separator))
		if dirAttr, ok := attributes[dir]; ok {
			if dirAttr.NameModeMap == nil {
				log.Panicf("parent dir has no NameModeMap: %q", dir)
			}
			if r.Deletion() {
				dirAttr.NameModeMap[basename] = 0, false
			} else {
				dirAttr.NameModeMap[basename] = r.Mode &^ 0777
			}
		}

		old := attributes[r.Path]
		if old == nil {
			old = &r
			attributes[r.Path] = old
		}
		old.Merge(r)
	}
}

func (me *FsServer) updateAttrs(infos []*FileAttr) {
	defer me.verify()

	me.attrCacheMutex.Lock()
	defer me.attrCacheMutex.Unlock()

	updateAttributeMap(me.attrCache, infos)
}

func (me *FsServer) updateHashes(infos []*FileAttr) {
	defer me.verify()

	me.hashCacheMutex.Lock()
	defer me.hashCacheMutex.Unlock()

	for _, r := range infos {
		name := r.Path
		if r.Deletion() || r.Link != "" {
			me.hashCache[name] = "", false
			me.hashBusyMap[name] = false, false
		}
		if r.Hash != "" {
			me.hashCache[name] = r.Hash
			me.hashBusyMap[name] = false, false
		}
	}
}

func (me *FsServer) dropHash(name string) {
	me.hashCacheMutex.Lock()
	defer me.hashCacheMutex.Unlock()
	me.hashCache[name] = "", false
	me.hashCacheCond.Broadcast()
}

func (me *FsServer) getHash(name string) (hash string) {
	fullPath := me.path(name)

	me.hashCacheMutex.RLock()
	hash = me.hashCache[name]
	me.hashCacheMutex.RUnlock()

	if hash != "" {
		return hash
	}

	me.hashCacheMutex.Lock()
	defer me.hashCacheMutex.Unlock()
	for me.hashBusyMap[name] && me.hashCache[name] == "" {
		me.hashCacheCond.Wait()
	}

	hash = me.hashCache[name]
	if hash != "" {
		return hash
	}
	me.hashBusyMap[name] = true
	me.hashCacheMutex.Unlock()

	// TODO - /usr should be configurable.
	if HasDirPrefix(fullPath, "/usr") && !HasDirPrefix(fullPath, "/usr/local") {
		hash = me.contentCache.SaveImmutablePath(fullPath)
	} else {
		hash = me.contentCache.SavePath(fullPath)
	}

	me.hashCacheMutex.Lock()
	me.hashCache[name] = hash
	me.hashBusyMap[name] = false, false
	me.hashCacheCond.Broadcast()

	return hash
}

func (me *FsServer) refreshAttributeCache(prefix string) FileSet {
	me.attrCacheMutex.Lock()
	defer me.attrCacheMutex.Unlock()

	if prefix != "" && prefix[0] == '/' {
		panic("leading /")
	}

	updated := []*FileAttr{}
	for key, attr := range me.attrCache {
		// TODO -should just do everything?
		if !strings.HasPrefix(key, prefix) {
			continue
		}

		fi, _ := os.Lstat(me.path(key))
		if fi == nil && !attr.Deletion() {
			del := FileAttr{
				Path: key,
			}
			updated = append(updated, &del)
		}
		// TODO - does this handle symlinks corrrectly?
		if fi != nil && attr.FileInfo != nil && EncodeFileInfo(*attr.FileInfo) != EncodeFileInfo(*fi) {
			newEnt := FileAttr{
				Path:     key,
				FileInfo: fi,
			}
			me.dropHash(key)

			me.fillContent(&newEnt)
			updated = append(updated, &newEnt)
		}
	}

	fs := FileSet{updated}
	fs.Sort()
	for _, u := range fs.Files {
		copy := *u
		me.attrCache[u.Path] = &copy
	}
	return fs
}

func (me *FsServer) copyCache() FileSet {
	me.attrCacheMutex.RLock()
	defer me.attrCacheMutex.RUnlock()

	dump := []*FileAttr{}
	for _, attr := range me.attrCache {
		copy := *attr
		dump = append(dump, &copy)
	}

	fs := FileSet{dump}
	fs.Sort()
	return fs
}
