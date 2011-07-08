package termite

import (
	"bytes"
	"os"
	"fmt"
	"crypto"
	"hash"
	"path/filepath"
	"log"
	"io"
	"io/ioutil"
)


// Content based addressing cache.
//
// TODO - a successful GetAttr() will often be followed by a read.  we
// should have a small LRU cache for the content so we can serve the
// contents from memory.
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
	dest   *os.File
	hash   []byte
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
	me.hasher.Write(p[:n])
	return n, err
}

func (me *HashWriter) Close() os.Error {
	err := me.dest.Close()

	if err != nil {
		return err
	}
	src := me.dest.Name()
	dir, _ := filepath.Split(src)
	sum := me.hasher.Sum()
	sumpath := HashPath(dir, sum)

	log.Printf("saving hash %x\n", sum)
	err = os.Rename(src, sumpath)
	if err != nil {
		if fi, _ := os.Lstat(sumpath); fi == nil {
			log.Println("already have", sumpath)
			os.Remove(src)
		}
	}
	return err
}

func (me *DiskFileCache) DestructiveSavePath(path string) (md5 []byte) {
	f, err := os.Open(path)
	if err != nil {
		return nil
	}
	h := crypto.MD5.New()
	_, err = io.Copy(h, f)
	if err != nil {
		log.Fatal("DestructiveSavePath:", err)
	}

	s := h.Sum()
	p := me.Path(s)
	err = os.Rename(path, p)
	if err != nil {
		if fi, _ := os.Lstat(p); fi != nil {
			os.Remove(p)
			return s
		}
		log.Fatal("DestructiveSavePath:", err)
	}
	return s
}

func (me *DiskFileCache) SavePath(path string) (md5 []byte) {
	f, err := os.Open(path)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	return me.SaveStream(f)
}

func (me *DiskFileCache) Save(content []byte) (md5 []byte) {
	buf := bytes.NewBuffer(content)
	return me.SaveStream(buf)
}

func (me *DiskFileCache) SaveStream(input io.Reader) (md5 []byte) {
	dup := NewHashWriter(me.dir, crypto.MD5)
	_, err := io.Copy(dup, input)
	if err != nil {
		log.Fatal(err)
	}
	err = dup.Close()
	if err != nil {
		log.Fatal(err)
	}
	return dup.hasher.Sum()
}
