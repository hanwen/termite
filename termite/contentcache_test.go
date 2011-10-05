package termite

import (
	"bytes"
	"crypto"
	"os"
	"testing"
	"io/ioutil"
)

func TestDiskCache(t *testing.T) {
	content := []byte("hello")

	d, _ := ioutil.TempDir("", "term-cc")
	defer os.RemoveAll(d)

	cache := NewContentCache(d)
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

func TestLocalPath(t *testing.T) {
	f, _ := ioutil.TempFile("", "")
	defer os.Remove(f.Name())
	_, err := f.Write([]byte("hello"))
	if err != nil {
		t.Fatal(err)
	}
	f.Close()

	d, _ := ioutil.TempDir("", "term-cc")
	defer os.RemoveAll(d)
	cache := NewContentCache(d)

	saved := cache.SaveImmutablePath(f.Name())
	/*	content := cache.inMemoryCache.Get(f.Name())
		if string(md5(content.([]byte))) != string(saved) {
			t.Fatal("hash mismatch")
		}
	*/
	if f.Name() != cache.Path(saved) {
		t.Error("path mismatch")
	}
}

func TestDiskCacheDestructiveSave(t *testing.T) {
	content := []byte("hello")

	d, _ := ioutil.TempDir("", "term-cc")
	defer os.RemoveAll(d)
	cache := NewContentCache(d)

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

func TestDiskCacheStream(t *testing.T) {
	content := []byte("hello")

	d, _ := ioutil.TempDir("", "term-cc")
	defer os.RemoveAll(d)
	cache := NewContentCache(d)

	h := crypto.MD5.New()
	h.Write(content)
	checksum := string(h.Sum())

	b := bytes.NewBuffer(content)
	savedSum := cache.SaveStream(b)
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

func TestDiskCacheStreamReturnContent(t *testing.T) {
	content := make([]byte, _BUFSIZE-1)
	for i:= range content {
		content[i] = 'x'
	}

	d, _ := ioutil.TempDir("", "term-cc")
	defer os.RemoveAll(d)
	cache := NewContentCache(d)

	b := bytes.NewBuffer(content)
	hash := cache.SaveStream(b)

	if !cache.inMemoryCache.Has(hash) {
		t.Errorf("should have key %x", hash)
	}

	content = make([]byte, _BUFSIZE+1)
	for i:= range content {
		content[i] = 'y'
	}

	b = bytes.NewBuffer(content)
	hash = cache.SaveStream(b)
	if cache.inMemoryCache.Has(hash) {
		t.Errorf("should not have key %x %v", hash, cache.inMemoryCache.Get(hash))
	}
}
