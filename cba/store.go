package cba

import (
	"crypto"
	"io"
	"io/ioutil"
	"log"
	"os"
	"sync"
	"time"

	"github.com/hanwen/termite/fastpath"
	"github.com/hanwen/termite/stats"
)

// Content based addressing cache.
type Store struct {
	// Should not change option values after initalizing.
	Options *StoreOptions

	timings    *stats.TimerStats
	throughput *stats.PeriodicSampler

	mutex         sync.Mutex
	bytesServed   stats.MemCounter
	bytesReceived stats.MemCounter
}

type StoreOptions struct {
	Hash       crypto.Hash
	Dir        string
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

	c := &Store{
		Options:  options,
		timings:  stats.NewTimerStats(),
	}
	c.initThroughputSampler()
	return c
}

func (st *Store) HashType() crypto.Hash {
	return st.Options.Hash
}

func hexDigit(b byte) byte {
	if b < 10 {
		return byte('0' + b)
	}
	return b + 'a' - 10
}

func HashPath(dir string, hash string) string {
	hex := make([]byte, 2*len(hash))
	j := 0
	for i := 0; i < len(hash); i++ {
		hex[j] = hexDigit(hash[i] >> 4)
		hex[j+1] = hexDigit(hash[i] & 0x0f)
		j += 2
	}
	prefixDir := fastpath.Join(dir, string(hex[:2]))
	if err := os.MkdirAll(prefixDir, 0700); err != nil {
		log.Fatal("MkdirAll error:", err)
	}
	return fastpath.Join(prefixDir, string(hex[2:]))
}

func (st *Store) HasHash(hash string) bool {
	_, err := os.Lstat(st.Path(hash))
	return err == nil
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

	size, _ := io.Copy(h, f)

	s := string(h.Sum(nil))
	if st.HasHash(s) {
		os.Remove(path)
		return s, nil
	}

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

	st.AddTiming("DestructiveSave", int(size), dt)

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


func (st *Store) Save(content []byte) (hash string) {
	writer := st.NewHashWriter()
	err := writer.WriteClose(content)
	if err != nil {
		log.Println("saveViaMemory:", err)
		return ""
	}
	hash = writer.Sum()
	return hash
}

func (st *Store) SaveStream(input io.Reader, size int64) (hash string) {
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
