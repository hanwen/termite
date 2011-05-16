package rpcfs

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"io/ioutil"
	"io"
	"strings"
)

var _ = fmt.Println

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
	Chunks [][]byte
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

func (me *FsServer) stamp(name string, st *Stamp) os.Error {
	fi, err := os.Stat(me.getPath(name))
	if fi != nil {
		st.Ctime = fi.Ctime_ns
		st.Mtime = fi.Mtime_ns
	}
	return err
}

func ReadChunks(r io.Reader, size int) (chunks [][]byte, err os.Error) {
	for {
		chunk := make([]byte, size)
		n, err := r.Read(chunk)
		if n > 0 {
			chunk = chunk[:n]
			chunks = append(chunks, chunk)
		}
		if err == os.EOF {
			err = nil
			break
		}
		if err != nil || n == 0 {
			break
		}
	}
	return chunks, err
}

func (me *FsServer) FileContent(req *DirRequest, rep *ContentResponse) (os.Error) {
	log.Println("FileContent", req)

	f, err := os.Open(me.getPath(req.Name))
	if err != nil {
		return err
	}
	defer f.Close()
	
	chunksize := 128 * (1<<10)
	chunks, err := ReadChunks(f, chunksize)
	
	if err == nil {
		err = me.stamp(req.Name, &rep.Stamp)
		rep.Chunks = chunks
	}
	return err
}
