package rpcfs

import (
	"log"
	"os"
	"path/filepath"
	"io/ioutil"
	"strings"
)

type FsServer struct {
	Root string
}

type Stamp struct {
	Ctime int64
	Mtime int64
}

// TODO - should send timestamps for returns?
type ContentRequest struct {
	Name string
}
type ContentResponse struct {
	Stamp
	Data []byte
}

type DirRequest struct {
	Name string 
}

type DirResponse struct {
	Stamp
	Data     map[string]*os.FileInfo
	Symlinks map[string]string
}

func (me *FsServer) getPath(n string) string {
	if me.Root == "" {
		return n
	}
	return filepath.Join(me.Root, strings.TrimLeft(n, "/"))
}

func (me *FsServer) ReadDir(req *DirRequest, r *DirResponse) (os.Error) {
	d, e :=  ioutil.ReadDir(me.getPath(req.Name))
	log.Println("ReadDir", req)
	r.Data = make(map[string]*os.FileInfo)
	r.Symlinks = make(map[string]string)
	for _, v := range d {
		r.Data[v.Name] = v
		if v.IsSymlink() {
			// TODO - error handling.
			r.Symlinks[v.Name], _ = os.Readlink(filepath.Join(req.Name, v.Name))
		}
	}

	me.stamp(req.Name, &r.Stamp)
	return e
}

func (me *FsServer) stamp(name string, st *Stamp) {
	fi, _ := os.Stat(name)
	st.Ctime = fi.Ctime_ns
	st.Mtime = fi.Mtime_ns
}

func (me *FsServer) FileContent(req *DirRequest, cr *ContentResponse) (os.Error) {
	log.Println("FileContent", req)
	d, e := ioutil.ReadFile(me.getPath(req.Name))
	cr.Data = d

	me.stamp(req.Name, &cr.Stamp)
	return e
}
