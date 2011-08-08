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
	contentServer *ContentServer
	contentCache  *ContentCache
	Root          string
	excluded      map[string]bool

	multiplyPaths func(string)[]string

	hashCacheMutex sync.RWMutex
	// TODO - should use string (immutable) throughout for storing MD5 signatures.
	hashCache map[string][]byte

	attrCacheMutex sync.RWMutex
	attrCache map[string]FileAttr
}

func NewFsServer(root string, cache *ContentCache, excluded []string) *FsServer {
	fs := &FsServer{
		contentCache:  cache,
		contentServer: &ContentServer{Cache: cache},
		Root:          root,
		hashCache:     make(map[string][]byte),
		attrCache:     make(map[string]FileAttr),
	}

	fs.excluded = make(map[string]bool)
	for _, e := range excluded {
		fs.excluded[e] = true
	}
	return fs
}

type AttrRequest struct {
	Name string
}

type FileAttr struct {
	Path string
	*os.FileInfo
	fuse.Status
	Hash    []byte
	Link    string
	Content []byte // optional.
}

type AttrResponse struct {
	Attrs []FileAttr 
}

func (me FileAttr) String() string {
	id := ""
	if me.Hash != nil {
		id = fmt.Sprintf(" sz %d", me.FileInfo.Size)
	}
	if me.Link != "" {
		id = fmt.Sprintf(" -> %s", me.Link)
	}
	if me.Deletion() {
		id = " (del)"
	}
	return fmt.Sprintf("%s%s", me.Path, id)
}

func (me FileAttr) Deletion() bool {
	return me.Status == fuse.ENOENT
}

type DirRequest struct {
	Name string
}

type DirResponse struct {
	NameModeMap map[string]uint32
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
	return e
}

func (me *FsServer) GetAttr(req *AttrRequest, rep *AttrResponse) os.Error {
	log.Println("GetAttr req", req.Name)
	names := []string{}
	if me.multiplyPaths != nil {
		names = me.multiplyPaths(req.Name)
	} else {
		names = append(names, req.Name)
	}
	for _, n := range names {
		a := FileAttr{}
		err := me.oneGetAttr(n, &a)
		if err != nil {
			return err
		}
		if a.Hash != nil {
			log.Printf("GetAttr %s %v %x", n, a, a.Hash)
		}
		rep.Attrs = append(rep.Attrs, a)
	}
	return nil
}

func (me *FsServer) oneGetAttr(name string, rep *FileAttr) os.Error {
	rep.Path = name
	// TODO - this is not a good security measure, as we are not
	// checking the prefix; someone might directly ask for
	// /forbidden/subdir/
	if me.excluded[name] {
		rep.Status = fuse.ENOENT
		return nil
	}

	me.attrCacheMutex.RLock()
	attr, ok := me.attrCache[name]
	me.attrCacheMutex.RUnlock()

	if ok {
		*rep = attr
		return nil
	}
	me.attrCacheMutex.Lock()
	defer me.attrCacheMutex.Unlock()
	attr, ok = me.attrCache[name]
	if ok {
		*rep = attr
		return nil
	}

	fi, err := os.Lstat(me.path(name))
	rep.FileInfo = fi
	rep.Status = fuse.OsErrorToErrno(err)
	rep.Path = name
	if fi != nil {
		me.fillContent(rep)
	}

	me.attrCache[name] = *rep
	return nil
}

func (me *FsServer) fillContent(rep *FileAttr) {
	if rep.FileInfo.IsSymlink() {
		rep.Link, _ = os.Readlink(rep.Path)
	}
	if rep.FileInfo.IsRegular() {
		// TODO - saving the content easily overflows memory
		// on 32-bit.
		rep.Hash, _ = me.getHash(rep.Path)
	}
}

func (me *FsServer) updateFiles(infos []FileAttr) {
	me.updateHashes(infos)
	me.updateAttrs(infos)
}

func (me *FsServer) updateAttrs(infos []FileAttr) {
	me.attrCacheMutex.Lock()
	defer me.attrCacheMutex.Unlock()

	for _, r := range infos {
		name := r.Path
		me.attrCache[name] = r
	}
}

func (me *FsServer) updateHashes(infos []FileAttr) {
	me.hashCacheMutex.Lock()
	defer me.hashCacheMutex.Unlock()

	for _, r := range infos {
		name := r.Path
		if !r.Status.Ok() || r.Link != "" {
			me.hashCache[name] = nil, false
		}
		if r.Hash != nil {
			me.hashCache[name] = r.Hash
		}
	}
}

func (me *FsServer) getHash(name string) (hash []byte, content []byte) {
	fullPath := me.path(name)

	me.hashCacheMutex.RLock()
	hash = me.hashCache[name]
	me.hashCacheMutex.RUnlock()

	if hash != nil {
		return []byte(hash), nil
	}

	me.hashCacheMutex.Lock()
	defer me.hashCacheMutex.Unlock()
	hash = me.hashCache[name]
	if hash != nil {
		return []byte(hash), nil
	}

	// TODO - would it be better to not stop other hash lookups
	// from succeeding?

	// TODO - /usr should be configurable.
	if strings.HasPrefix(fullPath, "/usr") {
		hash, content = me.contentCache.SaveImmutablePath(fullPath)
	} else {
		hash, content = me.contentCache.SavePath(fullPath)
	}

	me.hashCache[name] = hash
	return hash, content
}

// TODO - decide between []FileAttr and []*FileAttr.
func (me *FsServer) refreshAttributeCache(prefix string) []FileAttr {
	me.attrCacheMutex.Lock()
	defer me.attrCacheMutex.Unlock()

	updated := []FileAttr{}
	entries := ListFilesRecursively(prefix)
	for key, attr := range me.attrCache {
		if HasDirPrefix(key, prefix) {
			_, ok := entries[key]
			if !ok && attr.Status.Ok() {
				del := FileAttr{
				Path: key,
				Status: fuse.ENOENT,
				}

				updated = append(updated, del)
			}
		}
	}

	for name, e := range entries {
		attr, ok := me.attrCache[name]
		newFi := e
		if ok && attr.FileInfo != nil && EncodeFileInfo(*attr.FileInfo) == EncodeFileInfo(e) { 
			continue
		}
		newEnt := FileAttr{
		Path: name,
		Status: fuse.OK,
		FileInfo: &newFi,
		}
		me.fillContent(&newEnt)
		updated = append(updated, newEnt)
	}

	for _, u := range updated {
		me.attrCache[u.Path] = u
	}
	return updated
}
