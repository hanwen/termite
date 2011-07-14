package termite

import (
	"fmt"
	"os"
	"path/filepath"
	"io/ioutil"
	)

// Caches remote files, keyed by server/full-path/os.FileInfo
type DiskFileCache struct {
	dir string
}

func NewDiskFileCache(dir string) *DiskFileCache {
	return &DiskFileCache{
	dir: dir,
	}
}

func (me *DiskFileCache) Path(server string, metadata os.FileInfo) string {
	dir, base := filepath.Split(metadata.Name)
	
	metadata.Name = ""
	key := fmt.Sprintf("%v-%s", metadata, dir)
	key = fmt.Sprintf("%x-%s", md5([]byte(key)), base)
	return filepath.Join(me.dir, server, key[:2], key[2:])
}

func (me *DiskFileCache) HasFile(server string, metadata os.FileInfo) bool {
	p := me.Path(server, metadata)
	fi, _ := os.Lstat(p)
	return fi != nil
}

func (me *DiskFileCache) SaveContents(content []byte, dest string) os.Error {
	d, _ := filepath.Split(dest)
	if fi, _ := os.Lstat(d); fi == nil {
		if err := os.MkdirAll(d, 0700); err != nil {
			return err
		}
	}
	
	f, err := ioutil.TempFile(me.dir, ".md5temp")
	if err != nil { return err }
	_, err = f.Write(content)
	if err != nil { return err }
	f.Close()
	if err != nil { return err }

	err = os.Rename(f.Name(), dest)
	if err != nil {
		if fi, _ := os.Lstat(dest); fi != nil {
			os.Remove(f.Name())
			return nil
		}
	}
	return err
}
	
