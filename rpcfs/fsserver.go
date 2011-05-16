package rpcfs

import (
	"os"
	"path/filepath"
	"io/ioutil"
	"strings"
)

type FsServer struct {
	Root string
}

// TODO - should send timestamps for returns?
type ContentRequest struct {
	Name string
}
type ContentResponse struct {
	Data []byte
}

type DirRequest struct {
	Name string 
}
type DirResponse struct {
	Data map[string]*os.FileInfo
}

type LinkRequest struct {
	Name string 
}
type LinkResponse struct {
	Data string
}

func (me *FsServer) getPath(n string) string {
	if me.Root == "" {
		return n
	}
	return filepath.Join(me.Root, strings.TrimLeft(n, "/"))
}

func (me *FsServer) ReadDir(req *DirRequest, r *DirResponse) (os.Error) {
	
	d, e :=  ioutil.ReadDir(me.getPath(req.Name))

	r.Data = make(map[string]*os.FileInfo)
	for _, v := range d {
		r.Data[v.Name] = v
	}
	return e
}

func (me *FsServer) Readlink(req *LinkRequest, cr *LinkResponse) (os.Error) {
	d, e := os.Readlink(me.getPath(req.Name))
	cr.Data = d
	return e
}

func (me *FsServer) FileContent(req *DirRequest, cr *ContentResponse) (os.Error) {
	d, e := ioutil.ReadFile(me.getPath(req.Name))
	cr.Data = d
	return e
}
