package cba

import (
	"crypto"
	"fmt"
	"hash"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"sync"
)

// Content based addressing cache.
type ContentCache struct {
	dir      string
	hashFunc crypto.Hash

	// Try to keep small files in memory.
	MemoryLimit   int64
	
	mutex         sync.Mutex
	cond          *sync.Cond
	faulting      map[string]bool
	have          map[string]bool
	inMemoryCache *LruCache

	memoryTries int
	memoryHits  int
}

// NewContentCache creates a content cache based in directory d.
// memorySize sets the maximum number of file contents to keep in
// memory.
func NewContentCache(d string, hash crypto.Hash) *ContentCache {
	if fi, _ := os.Lstat(d); fi == nil {
		err := os.MkdirAll(d, 0700)
		if err != nil {
			panic(err)
		}
	}

	c := &ContentCache{
		dir:           d,
		have:          ReadHexDatabase(d),
		inMemoryCache: NewLruCache(1024),
		faulting:      make(map[string]bool),
		hashFunc:      hash,
	}
	c.cond = sync.NewCond(&c.mutex)
	return c
}

// SetMemoryCacheSize readjusts the size of the in-memory content
// cache.  Not thread safe.
func (me *ContentCache) SetMemoryCacheSize(fileCount int, limit int) {
	if fileCount == 0 {
		me.inMemoryCache = nil
		return
	}
	if me.inMemoryCache.Size() != fileCount {
		me.inMemoryCache = NewLruCache(fileCount)
	}
	me.MemoryLimit = int64(limit)
}

func (me *ContentCache) MemoryHitRate() float64 {
	if me.memoryTries == 0 {
		return 0.0
	}
	return float64(me.memoryHits) / float64(me.memoryTries)
}

func HashPath(dir string, hash string) string {
	s := fmt.Sprintf("%x", hash)
	prefix := s[:2]
	name := s[2:]
	dst := filepath.Join(dir, prefix, name)
	prefixDir, _ := filepath.Split(dst)
	if err := os.MkdirAll(prefixDir, 0700); err != nil {
		log.Fatal("MkdirAll error:", err)
	}
	return dst
}

func (me *ContentCache) HasHash(hash string) bool {
	me.mutex.Lock()
	defer me.mutex.Unlock()

	return me.have[hash]
}

func (me *ContentCache) ContentsIfLoaded(hash string) []byte {
	me.mutex.Lock()
	defer me.mutex.Unlock()
	for me.faulting[hash] {
		me.cond.Wait()
	}
	me.memoryTries++
	if me.inMemoryCache == nil {
		return nil
	}
	c := me.inMemoryCache.Get(hash)
	if c != nil {
		me.memoryHits++
		return c.([]byte)
	}
	return nil
}

func (me *ContentCache) Path(hash string) string {
	return HashPath(me.dir, hash)
}

func (me *ContentCache) NewHashWriter() *HashWriter {
	return NewHashWriter(me.dir, me.hashFunc)
}

type HashWriter struct {
	hasher hash.Hash
	dest   *os.File
}

func NewHashWriter(dir string, hashfunc crypto.Hash) *HashWriter {
	me := &HashWriter{}
	tmp, err := ioutil.TempFile(dir, ".hashtemp")
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

func (me *HashWriter) Write(p []byte) (n int, err error) {
	n, err = me.dest.Write(p)
	me.hasher.Write(p[:n])
	return n, err
}

func (me *HashWriter) WriteClose(p []byte) (err error) {
	_, err = me.Write(p)
	if err != nil {
		return err
	}
	err = me.Close()
	return err
}

func (me *HashWriter) CopyClose(input io.Reader, size int64) error {
	_, err := io.CopyN(me, input, size)
	if err != nil {
		return err
	}
	err = me.Close()
	return err
}

func (me *HashWriter) Close() error {
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
		log.Fatal("Rename failed", err)
	}
	return err
}

const _BUFSIZE = 32 * 1024

func (me *ContentCache) DestructiveSavePath(path string) (hash string, err error) {
	var f *os.File
	f, err = os.Open(path)
	if err != nil {
		return "", err
	}
	before, _ := f.Stat()
	defer f.Close()

	h := me.hashFunc.New()

	var content []byte
	if before.Size < me.MemoryLimit {
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

	me.mutex.Lock()
	me.have[s] = true
	if content != nil && me.inMemoryCache != nil {
		me.inMemoryCache.Add(s, content)
	}
	me.mutex.Unlock()

	p := me.Path(s)
	err = os.Rename(path, p)
	if err != nil {
		log.Fatal("Rename failed", err)
	}
	f.Chmod(0444)
	after, _ := f.Stat()
	if after.Mtime_ns != before.Mtime_ns || after.Size != before.Size {
		log.Fatal("File changed during save", OsFileInfo(*before), OsFileInfo(*after))
	}
	return s, nil
}

func (me *ContentCache) SavePath(path string) (hash string) {
	f, err := os.Open(path)
	if err != nil {
		log.Println("SavePath:", err)
		return ""
	}
	defer f.Close()

	fi, _ := f.Stat()
	return me.SaveStream(f, fi.Size)
}

// FaultIn loads the data from disk into the memory cache.
func (me *ContentCache) FaultIn(hash string) {
	me.mutex.Lock()
	defer me.mutex.Unlock()
	if me.inMemoryCache == nil {
		return
	}
	for !me.inMemoryCache.Has(hash) && me.faulting[hash] {
		me.cond.Wait()
	}
	if me.inMemoryCache.Has(hash) {
		return
	}

	me.faulting[hash] = true
	me.mutex.Unlock()
	c, err := ioutil.ReadFile(me.Path(hash))
	me.mutex.Lock()
	if err != nil {
		log.Fatal("FaultIn:", err)
	}
	delete(me.faulting, hash)
	me.inMemoryCache.Add(hash, c)
	me.cond.Broadcast()
}

func (me *ContentCache) Save(content []byte) (hash string) {
	return me.saveViaMemory(content)
}

func (me *ContentCache) saveViaMemory(content []byte) (hash string) {
	writer := me.NewHashWriter()
	err := writer.WriteClose(content)
	if err != nil {
		log.Println("saveViaMemory:", err)
		return ""
	}
	hash = writer.Sum()

	me.mutex.Lock()
	defer me.mutex.Unlock()
	me.have[hash] = true
	if me.inMemoryCache != nil {
		me.inMemoryCache.Add(hash, content)
	}
	return hash
}

func (me *ContentCache) SaveStream(input io.Reader, size int64) (hash string) {
	if size < me.MemoryLimit {
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
	hash = dup.Sum()

	me.mutex.Lock()
	defer me.mutex.Unlock()
	me.have[hash] = true
	return hash
}

func (me *ContentCache) Fetch(fetcher func(start, end int) ([]byte, error)) (string, error) {
	chunkSize := 1 << 18

	var output *HashWriter
	written := 0
	for {
		content, err := fetcher(written, written+chunkSize)
		if err != nil {
			log.Println("FileContent error:", err)
			return "",  err
		}

		if len(content) < chunkSize && written == 0 {
			saved := me.Save(content)
			return saved, nil
		} else if output == nil {
			output = me.NewHashWriter()
		}

		n, err := output.Write(content)
		written += n
		if err != nil {
			return "", err
		}
		if len(content) < chunkSize {
			break
		}
	}

	output.Close()
	saved := string(output.hasher.Sum())
	me.mutex.Lock()
	defer me.mutex.Unlock()
	me.have[saved] = true
	return saved, nil
}

func (me *ContentCache) ServeChunk(start, end int, hash string) ([]byte, error) {
	if c := me.ContentsIfLoaded(hash); c != nil {
		if end > len(c) {
			end = len(c)
		}
		return c[start:end], nil
	}

	f, err := os.Open(me.Path(hash))
	if err != nil {
		return nil, err
	}
	defer f.Close()

	chunk := make([]byte, end-start)
	n, err := f.ReadAt(chunk, int64(start))
	chunk = chunk[:n]

	if err == io.EOF {
		err = nil
	}
	return chunk, err
}

func (me *ContentCache) Serve(req *ContentRequest, rep *ContentResponse) (err error) {
	rep.Chunk, err = me.ServeChunk(req.Start, req.End, req.Hash)
	return err
}

