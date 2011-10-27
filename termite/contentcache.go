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
}

func NewHashWriter(dir string, hashfunc crypto.Hash) *HashWriter {
	me := &HashWriter{}
	tmp, err := ioutil.TempFile(dir, ".md5temp")
	if err != nil {
		log.Panic("NewHashWriter: ", err)
	}

	me.dest = tmp
	me.hasher = hashfunc.New()
	return me
}

func (me *HashWriter) Sum() string {
	return string(me.hasher.Sum())
}

func (me *HashWriter) Write(p []byte) (n int, err os.Error) {
	n, err = me.dest.Write(p)
	me.hasher.Write(p[:n])
	return n, err
}

func (me *HashWriter) WriteClose(p []byte) (err os.Error) {
	_, err = me.Write(p)
	if err != nil {
		return err
	}
	err = me.Close()
	return err
}

func (me *HashWriter) CopyClose(input io.Reader, size int64) os.Error {
	_, err := io.CopyN(me, input, size)
	if err != nil {
		return err
	}
	err = me.Close()
	return err 
}

func (me *HashWriter) Close() os.Error {
	me.dest.Chmod(0444)
	err := me.dest.Close()
	if err != nil {
		return err
	}
	src := me.dest.Name()
	dir, _ := filepath.Split(src)
	sum := me.Sum()
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
	var content []byte 
	if before.Size < _MEMORY_LIMIT {
		content, err = ioutil.ReadAll(f)
		if err != nil {
			return "", err
		}

		h.Write(content)
	} else {
		io.Copy(h, f)
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

	fi, _ := f.Stat()
	return me.SaveStream(f, fi.Size)
}

func (me *ContentCache) SaveImmutablePath(path string) (md5 string) {
	hasher := crypto.MD5.New()

	f, err := os.Open(path)
	if err != nil {
		log.Println("SaveImmutablePath:", err)
		return ""
	}
	defer f.Close()

	var content []byte
	fi, _ := f.Stat()
	if fi.Size < _MEMORY_LIMIT {
		content, err = ioutil.ReadAll(f)
		if err != nil {
			log.Println("ReadAll:", err)
			return ""
		}
		hasher.Write(content)
	} else {
		_, err = io.Copy(hasher, f)
	}
	
	if err != nil && err != os.EOF {
		log.Println("io.Copy:", err)
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
	return me.SaveStream(buf, int64(len(content)))
}

func (me *ContentCache) saveViaMemory(content []byte) (md5 string) {
	writer := me.NewHashWriter()
	err := writer.WriteClose(content)
	if err != nil {
		log.Println("saveViaMemory:", err)
		return ""
	}
	hash := writer.Sum()
	
	if me.inMemoryCache != nil {
		me.mutex.Lock()
		defer me.mutex.Unlock()
		me.inMemoryCache.Add(hash, content)
	}
	return hash
}

const _MEMORY_LIMIT = 32*1024
func (me *ContentCache) SaveStream(input io.Reader, size int64) (md5 string) {
	if size < _MEMORY_LIMIT {
		b, err := ioutil.ReadAll(input)
		if int64(len(b)) != size {
			log.Panicf("SaveStream: short read: %v %v", len(b), err)
		}

		return me.saveViaMemory(b)
	}
	
	dup := me.NewHashWriter()
	err := dup.CopyClose(input, size)
	if err != nil {
		return ""
	}
	
	return dup.Sum()
}
