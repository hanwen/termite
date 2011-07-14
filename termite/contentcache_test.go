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

	d, _ := ioutil.TempDir("", "")
	defer os.RemoveAll(d)

	cache := NewContentCache(d)
	checksum := md5(content)

	f, _ := ioutil.TempFile("", "")
	f.Write(content)
	f.Close()

	savedSum, _ := cache.SavePath(f.Name())
	if string(savedSum) != string(checksum) {
		t.Fatal("mismatch")
	}
	if !cache.HasHash(checksum) {
		t.Fatal("path gone")
	}
}

func TestDiskCacheDestructiveSave(t *testing.T) {
	content := []byte("hello")

	d, _ := ioutil.TempDir("", "")
	defer os.RemoveAll(d)
	cache := NewContentCache(d)

	fn := d + "/test"
	err := ioutil.WriteFile(fn, content, 0644)
	if err != nil {
		t.Error(err)
	}

	saved, _ := cache.DestructiveSavePath(fn)
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

	saved, _ = cache.DestructiveSavePath(fn)
	if saved == nil || string(saved) != string(md5(content)) {
		t.Error("mismatch")
	}
	if fi, _ := os.Lstat(fn); fi != nil {
		t.Error("should have disappeared", fi)
	}
}

func TestDiskCacheStream(t *testing.T) {
	content := []byte("hello")

	d, _ := ioutil.TempDir("", "")
	defer os.RemoveAll(d)
	cache := NewContentCache(d)

	h := crypto.MD5.New()
	h.Write(content)
	checksum := h.Sum()

	b := bytes.NewBuffer(content)
	savedSum, _ := cache.SaveStream(b)
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
	for i, _ := range content {
		content[i] = 'x'
	}
	
	d, _ := ioutil.TempDir("", "")
	defer os.RemoveAll(d)
	cache := NewContentCache(d)

	b := bytes.NewBuffer(content)
	_, c := cache.SaveStream(b)
	if string(content) != string(c) {
		t.Error("content mismatch")
	}
	
	content = make([]byte, _BUFSIZE+1)
	for i, _ := range content {
		content[i] = 'y'
	}

	b = bytes.NewBuffer(content)
	_, c = cache.SaveStream(b)
	if c != nil {
		t.Error("should not save")
	}	
}
