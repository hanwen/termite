package termite

import (
	"fmt"
	"github.com/hanwen/go-fuse/fuse"
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
			panic(fmt.Sprintf("attrCache mismatch %q %#v", k, v))
		}
		if _, ok := me.attrCacheBusy[k]; ok {
			panic(fmt.Sprintf("attrCacheBusy and attrCache entry for %q", k))
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
		fs.excluded[e] = true
	}
	return fs
}

func (me FileAttr) String() string {
	id := ""
	if me.Hash != "" {
		id = fmt.Sprintf(" sz %d", me.FileInfo.Size)
	}
	if me.Link != "" {
		id = fmt.Sprintf(" -> %s", me.Link)
	}
	if me.Deletion() {
		id = " (del)"
	} else if !me.Status.Ok() {
		id = " " + me.Status.String()
	}
	if me.Status.Ok() {
		id += fmt.Sprintf(" m=%o", me.FileInfo.Mode)
	}

	return fmt.Sprintf("%s%s", me.Path, id)
}

func (me FileAttr) Deletion() bool {
	return me.Status == fuse.ENOENT
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

func (me *FsServer) ReadDir(req *DirRequest, r *DirResponse) os.Error {
	d, e := ioutil.ReadDir(me.path(req.Name))
	log.Println("ReadDir", req)
	r.NameModeMap = make(map[string]uint32)
	for _, v := range d {
		r.NameModeMap[v.Name] = v.Mode
	}
	r.Status = fuse.OsErrorToErrno(e)
	return nil
}

func (me *FsServer) GetAttr(req *AttrRequest, rep *AttrResponse) os.Error {
	log.Println("GetAttr req", req.Name)
	a := me.oneGetAttr(req.Name)
	if a.Hash != "" {
		log.Printf("GetAttr %v %x", a, a.Hash)
	}
	rep.Attrs = append(rep.Attrs, a)
	return nil
}

func (me *FsServer) oneGetAttr(name string) (rep *FileAttr) {
	defer me.verify()

	if me.excluded[name] {
		log.Printf("Denied access to excluded file %q", name)
		return &FileAttr{
			Path:   name,
			Status: fuse.ENOENT,
		}
	}

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

	p := me.path(name)
	fi, err := os.Lstat(p)
	rep = &FileAttr{
		FileInfo: fi,
		Status:   fuse.OsErrorToErrno(err),
		Path:     name,
	}

	// We don't want to expose the master's private files to the
	// world.
	if me.excludePrivate && fi != nil && fi.Mode&0077 == 0 {
		log.Printf("Denied access to private file %q", name)
		rep.FileInfo = nil
		rep.Status = fuse.EPERM
		fi = nil
	}

	if fi != nil {
		me.fillContent(rep)
	}

	me.attrCacheMutex.Lock()
	me.attrCache[name] = rep
	me.attrCacheCond.Broadcast()
	me.attrCacheBusy[name] = false, false
	return rep
}

func (me *FsServer) fillContent(rep *FileAttr) {
	if rep.FileInfo.IsSymlink() {
		rep.Link, _ = os.Readlink(rep.Path)
	}
	if rep.FileInfo.IsRegular() {
		rep.Hash = me.getHash(rep.Path)
		if rep.Hash == "" {
			// Typically happens if we want to open /etc/shadow as normal user.
			rep.Status = fuse.EPERM
		}
	}
}

func (me *FsServer) updateFiles(infos []*FileAttr) {
	me.updateHashes(infos)
	me.updateAttrs(infos)
}

func (me *FsServer) updateAttrs(infos []*FileAttr) {
	defer me.verify()

	me.attrCacheMutex.Lock()
	defer me.attrCacheMutex.Unlock()

	for _, r := range infos {
		me.attrCache[r.Path] = r
	}
}

func (me *FsServer) updateHashes(infos []*FileAttr) {
	defer me.verify()

	me.hashCacheMutex.Lock()
	defer me.hashCacheMutex.Unlock()

	for _, r := range infos {
		name := r.Path
		if !r.Status.Ok() || r.Link != "" {
			me.hashCache[name] = "", false
			me.hashBusyMap[name] = false, false
		}
		if r.Hash != "" {
			me.hashCache[name] = r.Hash
			me.hashBusyMap[name] = false, false
		}
	}
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

func (me *FsServer) refreshAttributeCache(prefix string) []*FileAttr {
	me.attrCacheMutex.Lock()
	defer me.attrCacheMutex.Unlock()

	updated := []*FileAttr{}
	for key, attr := range me.attrCache {
		// TODO -should just do everything?
		if !HasDirPrefix(key, prefix) {
			continue
		}

		fi, _ := os.Lstat(me.path(key))
		if fi == nil && attr.Status.Ok() {
			del := FileAttr{
				Path:   key,
				Status: fuse.ENOENT,
			}
			updated = append(updated, &del)
		}
		if fi != nil && attr.FileInfo != nil && EncodeFileInfo(*attr.FileInfo) != EncodeFileInfo(*fi) {
			newEnt := FileAttr{
				Path:     key,
				Status:   fuse.OK,
				FileInfo: fi,
			}
			me.fillContent(&newEnt)
			updated = append(updated, &newEnt)
		}
	}

	for _, u := range updated {
		copy := *u
		me.attrCache[u.Path] = &copy
	}
	return updated
}

func (me *FsServer) copyCache() []*FileAttr {
	me.attrCacheMutex.RLock()
	defer me.attrCacheMutex.RUnlock()

	dump := []*FileAttr{}
	for _, attr := range me.attrCache {
		copy := *attr
		dump = append(dump, &copy)
	}

	return dump
}
