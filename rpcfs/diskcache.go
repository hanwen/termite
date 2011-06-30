package rpcfs

import (
	"os"
	"fmt"
	"crypto"
	"hash"
	"path/filepath"
	"log"
	"io"
	"io/ioutil"
	
)

type DiskFileCache struct {
	dir string
}

func NewDiskFileCache(d string) *DiskFileCache {
	if fi, _ := os.Lstat(d); fi == nil {
		err := os.MkdirAll(d, 0700)
		if err != nil {
			panic(err)
		}
	}
	return &DiskFileCache{dir: d}
}

func HashPath(dir string, md5 []byte) string {
	s := fmt.Sprintf("%x", md5)
	prefix := s[:2]
	name := s[2:]
	dst := filepath.Join(dir, prefix, name)
	prefixDir, _ := filepath.Split(dst)
	if err := os.MkdirAll(prefixDir, 0700); err != nil {
		log.Fatal("MkdirAll error:", err)
	}
	return dst
}


func (me *DiskFileCache) HasHash(hash []byte) bool {
	p := HashPath(me.dir, hash)
	_, err := os.Lstat(p)
	return err == nil
}

func (me *DiskFileCache) Path(hash []byte) string {
	return HashPath(me.dir, hash)
}

type HashWriter struct {
	hasher hash.Hash
	dest *os.File
	hash []byte
}

func NewHashWriter(dir string, hashfunc crypto.Hash) *HashWriter {
	me := &HashWriter{}
	tmp, err := ioutil.TempFile(dir, ".md5temp")
	if err != nil {
		panic(err)
		log.Fatal(err)
	}

	me.dest = tmp
	me.hasher = hashfunc.New()
	return me
}

func (me *HashWriter) Write(p []byte) (n int, err os.Error) {
	n, err = me.dest.Write(p)
	me.hasher.Write(p)
	return n, err
}

func (me *HashWriter) Close() os.Error {
	err := me.dest.Close()

	if err != nil {
		return err
	}
	src := me.dest.Name()
	dir, _ := filepath.Split(src)
	sumpath := HashPath(dir, me.hasher.Sum())
	if fi, _ := os.Lstat(sumpath); fi == nil {
		err = os.Rename(src, sumpath)
	} else {
		os.Remove(src)
	}
	return err
}

	
func (me *DiskFileCache) SavePath(path string) (md5 []byte) {
	f, err := os.Open(path)
	if err != nil {
		log.Fatal(err)
	}

	dup := NewHashWriter(me.dir, crypto.MD5)
	_, err = io.Copy(dup, f) 
	if err != nil {
		log.Fatal(err)
	}
	err = dup.Close()
	if err != nil {
		log.Fatal(err)
	}
	return dup.hasher.Sum()
}

func (me *DiskFileCache) Save(content []byte) (md5 []byte) {
	h := crypto.MD5.New()
	
	// TODO: make atomic. 
	h.Write(content)
	sum := h.Sum()
	name := me.Path(sum)
	fi, err := os.Lstat(name)
	if fi != nil {
		return sum
	}

	f, err := os.Create(name)
	if err != nil {
		log.Fatal("Create err:", err)
	}
	f.Write(content)
	f.Close()

	log.Printf("saved Hash %x\n", sum)
	return sum
}

