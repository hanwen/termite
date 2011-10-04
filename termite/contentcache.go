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
	"sync"
)

// Content based addressing cache.
type ContentCache struct {
	dir string

	mutex         sync.Mutex
	hashPathMap   map[string]string
	inMemoryCache *LruCache
}

// NewContentCache creates a content cache based in directory d.
// memorySize sets the maximum number of file contents to keep in
// memory.
func NewContentCache(d string) *ContentCache {
	if fi, _ := os.Lstat(d); fi == nil {
		err := os.MkdirAll(d, 0700)
		if err != nil {
			panic(err)
		}
	}

	return &ContentCache{
		dir:           d,
		hashPathMap:   make(map[string]string),
		inMemoryCache: NewLruCache(1024),
	}
}

// SetMemoryCacheSize readjusts the size of the in-memory content
// cache.  Not thread safe.
func (me *ContentCache) SetMemoryCacheSize(fileCount int) {
	if fileCount == 0 {
		me.inMemoryCache = nil
		return
	}
	if me.inMemoryCache.Size() != fileCount {
		me.inMemoryCache = NewLruCache(fileCount)
	}
}

func HashPath(dir string, md5 string) string {
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

func (me *ContentCache) localPath(hash string) string {
	me.mutex.Lock()
	defer me.mutex.Unlock()

	return me.hashPathMap[hash]
}

func (me *ContentCache) HasHash(hash string) bool {
	me.mutex.Lock()
	defer me.mutex.Unlock()

	_, ok := me.hashPathMap[hash]
	if ok {
		return true
	}

	if me.inMemoryCache != nil {
		ok = me.inMemoryCache.Has(hash)
		if ok {
			return true
		}
	}

	p := HashPath(me.dir, hash)
	_, err := os.Lstat(p)
	return err == nil
}

func (me *ContentCache) ContentsIfLoaded(hash string) []byte {
	me.mutex.Lock()
	defer me.mutex.Unlock()
	if me.inMemoryCache == nil {
		return nil
	}
	c := me.inMemoryCache.Get(hash)
	if c != nil {
		return c.([]byte)
	}
	return nil
}

func (me *ContentCache) Path(hash string) string {
	p := me.localPath(hash)
	if p != "" {
		return p
	}
	return HashPath(me.dir, hash)
}

func (me *ContentCache) NewHashWriter() *HashWriter {
	return NewHashWriter(me.dir, crypto.MD5)
}

type HashWriter struct {
	hasher hash.Hash
	dest   *os.File
	hash   string
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
	me.dest.Chmod(0444)
	err := me.dest.Close()
	if err != nil {
		return err
	}
	src := me.dest.Name()
	dir, _ := filepath.Split(src)
	sum := string(me.hasher.Sum())
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

const _BUFSIZE = 32 * 1024

func (me *ContentCache) DestructiveSavePath(path string) (md5 string, err os.Error) {
	var f *os.File
	f, err = os.Open(path)
	if err != nil {
		return "", err
	}
	before, _ := f.Stat()
	defer f.Close()

	h := crypto.MD5.New()
	content, err := SavingCopy(h, f, _BUFSIZE)
	if err != nil {
		return "", err
	}
	s := string(h.Sum())
	if me.HasHash(s) {
		os.Remove(path)
		return s, nil
	}

	if content != nil && me.inMemoryCache != nil {
		me.mutex.Lock()
		me.inMemoryCache.Add(s, content)
		me.mutex.Unlock()
	}

	p := me.Path(s)
	err = os.Rename(path, p)
	if err != nil {
		if fi, _ := os.Lstat(p); fi != nil {
			os.Remove(p)
			return s, nil
		}
		return "", err
	}
	f.Chmod(0444)
	after, _ := f.Stat()
	if after.Mtime_ns != before.Mtime_ns || after.Size != before.Size {
		log.Fatal("File changed during save", before, after)
	}
	return s, nil
}

func (me *ContentCache) SavePath(path string) (md5 string) {
	f, err := os.Open(path)
	if err != nil {
		log.Println("SavePath:", err)
		return ""
	}
	defer f.Close()

	return me.SaveStream(f)
}

func (me *ContentCache) SaveImmutablePath(path string) (md5 string) {
	hasher := crypto.MD5.New()

	f, err := os.Open(path)
	if err != nil {
		log.Println("SaveImmutablePath:", err)
		return ""
	}
	defer f.Close()

	content, err := SavingCopy(hasher, f, 32*1024)
	if err != nil && err != os.EOF {
		log.Println("SavingCopy:", err)
		return ""
	}

	md5 = string(hasher.Sum())
	me.mutex.Lock()
	defer me.mutex.Unlock()
	me.hashPathMap[md5] = path
	if content != nil && me.inMemoryCache != nil {
		me.inMemoryCache.Add(md5, content)
	}

	log.Printf("hashed %s to %x", path, md5)
	return md5
}

func (me *ContentCache) Save(content []byte) (md5 string) {
	buf := bytes.NewBuffer(content)
	return me.SaveStream(buf)

}

func (me *ContentCache) SaveStream(input io.Reader) (md5 string) {
	dup := me.NewHashWriter()
	content, err := SavingCopy(dup, input, _BUFSIZE)
	if err != nil {
		log.Println("SaveStream:", err)
		return ""
	}
	err = dup.Close()
	if err != nil {
		log.Println("dup.Close:", err)
		return ""
	}
	hash := string(dup.hasher.Sum())

	if content != nil && me.inMemoryCache != nil {
		me.mutex.Lock()
		defer me.mutex.Unlock()
		me.inMemoryCache.Add(hash, content)
	}

	return hash
}
