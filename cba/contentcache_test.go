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

func TestContentCache(t *testing.T) {
	content := []byte("hello")

	d, _ := ioutil.TempDir("", "term-cc")
	defer os.RemoveAll(d)

	cache := NewContentCache(d, hashFunc)
	checksum := string(md5(content))

	f, _ := ioutil.TempFile("", "")
	f.Write(content)
	f.Close()

	savedSum := cache.SavePath(f.Name())
	if savedSum != checksum {
		t.Fatal("mismatch", savedSum, checksum)
	}
	if !cache.HasHash(checksum) {
		t.Fatal("path gone")
	}
}

func TestContentCacheDestructiveSave(t *testing.T) {
	content := []byte("hello")

	d, _ := ioutil.TempDir("", "term-cc")
	defer os.RemoveAll(d)
	cache := NewContentCache(d, hashFunc)

	fn := d + "/test"
	err := ioutil.WriteFile(fn, content, 0644)
	if err != nil {
		t.Error(err)
	}

	saved, err := cache.DestructiveSavePath(fn)
	check(err)
	if string(saved) != string(md5(content)) {
		t.Error("mismatch")
	}

	if !cache.HasHash(md5(content)) {
		t.Error("fail")
	}

	// Again.
	err = ioutil.WriteFile(fn, content, 0644)
	if err != nil {
		t.Error(err)
	}

	saved, err = cache.DestructiveSavePath(fn)
	check(err)
	if saved == "" || saved != md5(content) {
		t.Error("mismatch")
	}
	if fi, _ := os.Lstat(fn); fi != nil {
		t.Error("should have disappeared", fi)
	}
}

func TestContentCacheStream(t *testing.T) {
	content := []byte("hello")

	d, _ := ioutil.TempDir("", "term-cc")
	defer os.RemoveAll(d)
	cache := NewContentCache(d, hashFunc)

	h := crypto.MD5.New()
	h.Write(content)
	checksum := string(h.Sum())

	savedSum := cache.Save(content)
	if string(savedSum) != string(md5(content)) {
		t.Fatal("mismatch")
	}
	if !cache.HasHash(checksum) {
		t.Fatal("path gone")
	}

	data, err := ioutil.ReadFile(cache.Path(checksum))
	if err != nil {
		t.Fatal(err)
	}

	if bytes.Compare(data, content) != 0 {
		t.Error("compare.")
	}
}

func TestContentCacheStreamReturnContent(t *testing.T) {
	d, _ := ioutil.TempDir("", "term-cc")
	cache := NewContentCache(d, hashFunc)
	cache.SetMemoryCacheSize(100, 1024)
	content := make([]byte, cache.MemoryLimit-1)
	for i := range content {
		content[i] = 'x'
	}

	defer os.RemoveAll(d)

	hash := cache.Save(content)

	if !cache.inMemoryCache.Has(hash) {
		t.Errorf("should have key %x", hash)
	}

	content = make([]byte, cache.MemoryLimit+1)
	for i := range content {
		content[i] = 'y'
	}

	f, _ := ioutil.TempFile("", "term-cc")
	err := ioutil.WriteFile(f.Name(), content, 0644)
	check(err)
	hash = cache.SavePath(f.Name())
	if cache.inMemoryCache.Has(hash) {
		t.Errorf("should not have key %x %v", hash, cache.inMemoryCache.Get(hash))
	}
}
