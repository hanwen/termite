package cba

import (
	"bytes"
	"crypto"
	md5pkg "crypto/md5"
	"io/ioutil"
	"os"
	"testing"
)

var hashFunc = crypto.MD5

func md5(c []byte) string {
	h := md5pkg.New()
	h.Write(c)
	return string(h.Sum())
}

func check(err error) {
	if err != nil {
		panic(err)
	}
}
type ccTestCase struct {
	dir string
	cache *ContentCache
	options  *ContentCacheOptions
}

func newCcTestCase() (*ccTestCase) {
	d, _ := ioutil.TempDir("", "term-cc")
	opts := &ContentCacheOptions{
		Dir: d,
		Hash: hashFunc,
		MemCount: 10,
		MemMaxSize: 1024,
	}	
	cache := NewContentCache(opts)

	return &ccTestCase{d, cache, opts}
}

func (me *ccTestCase) Clean() {
	os.RemoveAll(me.dir)
}

func TestContentCache(t *testing.T) {
	tc := newCcTestCase()
	defer tc.Clean()
	content := []byte("hello")

	checksum := string(md5(content))

	f, _ := ioutil.TempFile("", "")
	f.Write(content)
	f.Close()

	savedSum := tc.cache.SavePath(f.Name())
	if savedSum != checksum {
		t.Fatal("mismatch", savedSum, checksum)
	}
	if !tc.cache.HasHash(checksum) {
		t.Fatal("path gone")
	}
}

func TestContentCacheDestructiveSave(t *testing.T) {
	tc := newCcTestCase()
	defer tc.Clean()
	d := tc.dir

	content := []byte("hello")
	fn := d + "/test"
	err := ioutil.WriteFile(fn, content, 0644)
	if err != nil {
		t.Error(err)
	}

	saved, err := tc.cache.DestructiveSavePath(fn)
	check(err)
	if string(saved) != string(md5(content)) {
		t.Error("mismatch")
	}

	if !tc.cache.HasHash(md5(content)) {
		t.Error("fail")
	}

	// Again.
	err = ioutil.WriteFile(fn, content, 0644)
	if err != nil {
		t.Error(err)
	}

	saved, err = tc.cache.DestructiveSavePath(fn)
	check(err)
	if saved == "" || saved != md5(content) {
		t.Error("mismatch")
	}
	if fi, _ := os.Lstat(fn); fi != nil {
		t.Error("should have disappeared", fi)
	}
}

func TestContentCacheStream(t *testing.T) {
	tc := newCcTestCase()
	defer tc.Clean()
	content := []byte("hello")

	h := crypto.MD5.New()
	h.Write(content)
	checksum := string(h.Sum())

	savedSum := tc.cache.Save(content)
	if string(savedSum) != string(md5(content)) {
		t.Fatal("mismatch")
	}
	if !tc.cache.HasHash(checksum) {
		t.Fatal("path gone")
	}

	data, err := ioutil.ReadFile(tc.cache.Path(checksum))
	if err != nil {
		t.Fatal(err)
	}

	if bytes.Compare(data, content) != 0 {
		t.Error("compare.")
	}
}

func TestContentCacheStreamReturnContent(t *testing.T) {
	tc := newCcTestCase()
	defer tc.Clean()
	content := make([]byte, tc.options.MemMaxSize-1)
	for i := range content {
		content[i] = 'x'
	}

	hash := tc.cache.Save(content)

	if !tc.cache.inMemoryCache.Has(hash) {
		t.Errorf("should have key %x", hash)
	}

	content = make([]byte, tc.options.MemMaxSize+1)
	for i := range content {
		content[i] = 'y'
	}

	f, _ := ioutil.TempFile("", "term-cc")
	err := ioutil.WriteFile(f.Name(), content, 0644)
	check(err)
	hash = tc.cache.SavePath(f.Name())
	if tc.cache.inMemoryCache.Has(hash) {
		t.Errorf("should not have key %x %v", hash, tc.cache.inMemoryCache.Get(hash))
	}
}
