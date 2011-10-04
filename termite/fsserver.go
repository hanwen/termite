package termite

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"
)

var _ = fmt.Println

type FsServer struct {
	contentServer  *ContentServer
	contentCache   *ContentCache
	Root           string
	excluded       map[string]bool
	excludePrivate bool
	attr           *AttributeCache
}

func NewFsServer(root string, cache *ContentCache, excluded []string) *FsServer {
	me := &FsServer{
		contentCache:  cache,
		contentServer: &ContentServer{Cache: cache},
		Root:          root,
		excludePrivate: true,
	}
	me.attr = NewAttributeCache(func(n string)*FileAttr {
		return me.uncachedGetAttr(n)
		},
		func (n string) *os.FileInfo {
			fi, _ := os.Lstat(me.path(n))
			return fi
		})
	me.excluded = make(map[string]bool)
	for _, e := range excluded {
		if e[0] == '/' {
			panic("leading slash")
		}
		me.excluded[e] = true
	}
	return me
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

	a := me.attr.Get(req.Name)
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

func (me *FsServer) fillContent(rep *FileAttr) {
	if rep.FileInfo.IsSymlink() {
		rep.Link, _ = os.Readlink(me.path(rep.Path))
	}
	if rep.FileInfo.IsRegular() {
		// TODO - /usr should be configurable.
		fullPath := me.path(rep.Path)
		if HasDirPrefix(fullPath, "/usr") && !HasDirPrefix(fullPath, "/usr/local") {
			rep.Hash = me.contentCache.SaveImmutablePath(fullPath)
		} else {
			rep.Hash = me.contentCache.SavePath(fullPath)
		}
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
	me.attr.Update(infos)
}

func (me *FsServer) refreshAttributeCache(prefix string) FileSet {
	return me.attr.Refresh(prefix)
}

func (me *FsServer) copyCache() FileSet {
	return me.attr.Copy()
}

