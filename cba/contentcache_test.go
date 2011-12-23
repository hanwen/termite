package cba

import (
	"bytes"
	"crypto"
	md5pkg "crypto/md5"
	"io/ioutil"
	"os"
	"testing"
)

func md5(c []byte) string {
	h := md5pkg.New()
	h.Write(c)
	return string(h.Sum(nil))
}

func check(err error) {
	if err != nil {
		panic(err)
	}
}

type ccTestCase struct {
	dir     string
	cache   *ContentCache
	options *ContentCacheOptions
}

func newCcTestCase() *ccTestCase {
	d, _ := ioutil.TempDir("", "term-cc")
	opts := &ContentCacheOptions{
		Dir:        d,
		MemCount:   10,
		MemMaxSize: 1024,
	}
	cache := NewContentCache(opts)

	return &ccTestCase{d, cache, opts}
}

func (me *ccTestCase) Clean() {
	os.RemoveAll(me.dir)
}

func TestHashWriter(t *testing.T) {
	tc := newCcTestCase()
	defer tc.Clean()

	content := []byte("hello")

	h := tc.cache.NewHashWriter()
	h.Write(content)
	h.Close()

	want := string(md5(content))

	saved := h.Sum()
	if saved != want {
		t.Fatalf("mismatch got %x want %x", saved, want)
	}
	if !tc.cache.HasHash(want) {
		t.Fatal("TestHashWriter: store does not have path")
	}
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
		t.Fatalf("mismatch got %x want %x", savedSum, checksum)
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
	want := md5(content)
	if saved == "" || saved != want {
		t.Error("mismatch got %x want %x", saved, want)
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
	checksum := string(h.Sum(nil))
	savedSum := tc.cache.Save(content)
	got := string(savedSum)
	want := string(md5(content))
	if got != want {
		t.Fatalf("mismatch %x %x", got, want)
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
