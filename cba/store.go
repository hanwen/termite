package cba

import (
	"crypto"
	md5pkg "crypto/md5"
	"crypto/sha1"
	"fmt"
	"github.com/hanwen/termite/stats"
	"hash"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"
)

var _ = md5pkg.New
var _ = sha1.New

// Content based addressing cache.
type Store struct {
	// Should not change option values after initalizing.
	Options *StoreOptions

	timings    *stats.TimerStats
	throughput *stats.PeriodicSampler

	mutex         sync.Mutex
	cond          *sync.Cond
	faulting      map[string]bool
	have          map[string]bool
	inMemoryCache *LruCache
	memoryTries   int
	memoryHits    int
	bytesServed   stats.MemCounter
	bytesReceived stats.MemCounter
}

type StoreOptions struct {
	Hash       crypto.Hash
	Dir        string
	MemCount   int
	MemMaxSize int64
}

// NewStore creates a content cache based in directory d.
// memorySize sets the maximum number of file contents to keep in
// memory.
func NewStore(options *StoreOptions) *Store {
	if options.Hash == 0 {
		options.Hash = crypto.MD5
	}
	if fi, _ := os.Lstat(options.Dir); fi == nil {
		err := os.MkdirAll(options.Dir, 0700)
		if err != nil {
			panic(err)
		}
	}

	log.Println("reading hex database", options.Dir)
	db := ReadHexDatabase(options.Dir)
	log.Println("done reading.")

	c := &Store{
		Options:  options,
		have:     db,
		faulting: make(map[string]bool),
		timings:  stats.NewTimerStats(),
	}
	c.initThroughputSampler()
	if options.MemCount > 0 {
		c.inMemoryCache = NewLruCache(options.MemCount)
		if options.MemMaxSize == 0 {
			options.MemMaxSize = 128 * 1024
		}
	}

	c.cond = sync.NewCond(&c.mutex)
	return c
}

func (st *Store) HashType() crypto.Hash {
	return st.Options.Hash
}

func (st *Store) MemoryHitRate() float64 {
	if st.memoryTries == 0 {
		return 0.0
	}
	return float64(st.memoryHits) / float64(st.memoryTries)
}

func (st *Store) MemoryHitAge() int {
	if st.inMemoryCache == nil {
		return 0
	}

	st.mutex.Lock()
	defer st.mutex.Unlock()
	return st.inMemoryCache.AverageAge()
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

func (st *Store) HasHash(hash string) bool {
	st.mutex.Lock()
	defer st.mutex.Unlock()

	return st.have[hash]
}

func (st *Store) ContentsIfLoaded(hash string) []byte {
	st.mutex.Lock()
	defer st.mutex.Unlock()
	for st.faulting[hash] {
		st.cond.Wait()
	}
	st.memoryTries++
	if st.inMemoryCache == nil {
		return nil
	}
	c := st.inMemoryCache.Get(hash)
	if c != nil {
		st.memoryHits++
		return c.([]byte)
	}
	return nil
}

func (st *Store) Path(hash string) string {
	return HashPath(st.Options.Dir, hash)
}

func (store *Store) NewHashWriter() *HashWriter {
	st := &HashWriter{cache: store}

	st.start = time.Now()
	tmp, err := ioutil.TempFile(store.Options.Dir, ".hashtemp")
	if err != nil {
		log.Panic("NewHashWriter: ", err)
	}

	st.dest = tmp
	st.hasher = store.Options.Hash.New()
	return st
}

type HashWriter struct {
	start  time.Time
	hasher hash.Hash
	dest   *os.File
	cache  *Store
	size   int
}

func (st *HashWriter) Sum() string {
	return string(st.hasher.Sum(nil))
}

func (st *HashWriter) Write(p []byte) (n int, err error) {
	n, err = st.dest.Write(p)
	st.hasher.Write(p[:n])
	st.size += n
	return n, err
}

func (st *HashWriter) WriteClose(p []byte) (err error) {
	_, err = st.Write(p)
	if err != nil {
		return err
	}
	err = st.Close()
	return err
}

func (st *HashWriter) CopyClose(input io.Reader, size int64) error {
	_, err := io.CopyN(st, input, size)
	if err != nil {
		return err
	}
	err = st.Close()
	return err
}

func (st *HashWriter) Close() error {
	st.dest.Chmod(0444)
	err := st.dest.Close()
	if err != nil {
		return err
	}
	src := st.dest.Name()
	dir, _ := filepath.Split(src)
	sum := st.Sum()
	sumpath := HashPath(dir, sum)

	log.Printf("saving hash %x\n", sum)
	err = os.Rename(src, sumpath)
	if err != nil {
		log.Fatal("Rename failed", err)
	}

	st.cache.mutex.Lock()
	st.cache.have[sum] = true
	st.cache.mutex.Unlock()

	dt := time.Now().Sub(st.start)

	st.cache.AddTiming("Save", st.size, dt)

	return err
}

const _BUFSIZE = 32 * 1024

func (st *Store) TimingMessages() []string {
	return st.timings.TimingMessages()
}

func (st *Store) TimingMap() map[string]*stats.RpcTiming {
	return st.timings.Timings()
}

func (st *Store) DestructiveSavePath(path string) (hash string, err error) {
	start := time.Now()
	var f *os.File
	f, err = os.Open(path)
	if err != nil {
		return "", err
	}
	before, _ := f.Stat()
	defer f.Close()

	h := st.Options.Hash.New()

	var content []byte
	var size int
	if before.Size() < st.Options.MemMaxSize {
		content, err = ioutil.ReadAll(f)
		if err != nil {
			return "", err
		}

		size, _ = h.Write(content)
	} else {
		sz, _ := io.Copy(h, f)
		size = int(sz)
	}

	s := string(h.Sum(nil))
	if st.HasHash(s) {
		os.Remove(path)
		return s, nil
	}

	st.mutex.Lock()
	st.have[s] = true
	if content != nil && st.inMemoryCache != nil {
		st.inMemoryCache.Add(s, content)
	}
	st.mutex.Unlock()

	p := st.Path(s)
	err = os.Rename(path, p)
	if err != nil {
		log.Fatal("Rename failed", err)
	}
	f.Chmod(0444)
	after, _ := f.Stat()
	if !after.ModTime().Equal(before.ModTime()) || after.Size() != before.Size() {
		log.Fatal("File changed during save", before, after)
	}

	dt := time.Now().Sub(start)

	st.AddTiming("DestructiveSave", size, dt)

	log.Printf("Saving %s as %x destructively", path, s)
	return s, nil
}

func (st *Store) SavePath(path string) (hash string) {
	f, err := os.Open(path)
	if err != nil {
		log.Println("SavePath:", err)
		return ""
	}
	defer f.Close()

	fi, _ := f.Stat()
	return st.SaveStream(f, fi.Size())
}

// FaultIn loads the data from disk into the memory cache.
func (st *Store) FaultIn(hash string) {
	st.mutex.Lock()
	defer st.mutex.Unlock()
	if st.inMemoryCache == nil {
		return
	}
	for !st.inMemoryCache.Has(hash) && st.faulting[hash] {
		st.cond.Wait()
	}
	if st.inMemoryCache.Has(hash) {
		return
	}

	st.faulting[hash] = true
	st.mutex.Unlock()
	c, err := ioutil.ReadFile(st.Path(hash))
	st.mutex.Lock()
	if err != nil {
		log.Fatal("FaultIn:", err)
	}
	delete(st.faulting, hash)
	st.inMemoryCache.Add(hash, c)
	st.cond.Broadcast()
}

func (st *Store) Save(content []byte) (hash string) {
	return st.saveViaMemory(content)
}

func (st *Store) saveViaMemory(content []byte) (hash string) {
	writer := st.NewHashWriter()
	err := writer.WriteClose(content)
	if err != nil {
		log.Println("saveViaMemory:", err)
		return ""
	}
	hash = writer.Sum()
	if st.inMemoryCache != nil {
		st.mutex.Lock()
		st.inMemoryCache.Add(hash, content)
		st.mutex.Unlock()
	}
	return hash
}

func (st *Store) SaveStream(input io.Reader, size int64) (hash string) {
	if size < st.Options.MemMaxSize {
		r := make([]byte, size)
		n, err := io.ReadAtLeast(input, r, int(size))
		if n != int(size) || err != nil {
			log.Panicf("SaveStream: short read: %v %v", n, err)
		}

		r = r[:n]
		return st.saveViaMemory(r)
	}

	dup := st.NewHashWriter()
	err := dup.CopyClose(input, size)

	if err != nil {
		return ""
	}

	return dup.Sum()
}

func (st *Store) AddTiming(name string, bytes int, dt time.Duration) {
	st.timings.Log("ContentStore."+name, dt)
	st.timings.LogN("ContentStore."+name+"Bytes", int64(bytes), dt)
}
