package termite

import (
	"crypto"
	"fmt"
	"io"
	"os"
	"io/ioutil"
	)

type DiskFileCache struct {
	dir string
}

func NewDiskFileCache(dir string) *DiskFileCache {
	return &DiskFileCache{
	dir: dir,
	}
}

func (me *DiskFileCache) HashPath(h []byte) string {
	return HashPath(me.dir, h)
}

func (me *DiskFileCache) Hash(server string, metadata os.FileInfo) []byte {
	key := fmt.Sprintf("%s:%v", server, metadata)
	
	h := crypto.MD5.New()
	io.WriteString(h, key)
	return h.Sum()
}

func (me *DiskFileCache) GetPath(server string, metadata os.FileInfo) string {
	hash := me.Hash(server, metadata)
	return HashPath(me.dir, hash)
}

func (me *DiskFileCache) HasFile(
	server string, path string, metadata os.FileInfo) bool {
	p := me.GetPath(server, metadata)
	fi, _ := os.Lstat(p)
	return fi != nil
}

func (me *DiskFileCache) SaveHash(content []byte, hash []byte) os.Error {
	f, err := ioutil.TempFile(me.dir, ".md5temp")
	if err != nil { return err }
	_, err = f.Write(content)
	if err != nil { return err }
	f.Close()
	if err != nil { return err }

	dest := HashPath(me.dir, hash)
	err = os.Rename(f.Name(), dest)
	if err != nil {
		if fi, _ := os.Lstat(dest); fi != nil {
			os.Remove(f.Name())
			return nil
		}
	}
	return err
}
	
