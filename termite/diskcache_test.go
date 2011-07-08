package termite

import (
	"bytes"
	"crypto"
	"os"
	"testing"
	"io/ioutil"
)

func md5(c []byte) []byte {
	h := crypto.MD5.New()
	h.Write(c)
	return h.Sum()
}

func TestDiskCache(t *testing.T) {
	content := []byte("hello")

	d, _ := ioutil.TempDir("", "")
	defer os.RemoveAll(d)
	
	cache := NewDiskFileCache(d)
	checksum := md5(content)

	f, _ := ioutil.TempFile("", "")
	f.Write(content)
	f.Close()

	savedSum := cache.SavePath(f.Name())
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
	cache := NewDiskFileCache(d)

	fn := d + "/test"
	err := ioutil.WriteFile(fn, content, 0644)
	if err != nil { t.Error(err) }

	saved := cache.DestructiveSavePath(fn)
	if string(saved) != string(md5(content)) {
		t.Error("mismatch")
	}

	if !cache.HasHash(md5(content)) {
		t.Error("fail")
	}

	err = ioutil.WriteFile(fn, content, 0644)
	if err != nil { t.Error(err) }

	saved = cache.DestructiveSavePath(fn)
	if saved == nil || string(saved) != string(md5(content)) {
		t.Error("mismatch")
	}
}

func TestDiskCacheStream(t *testing.T) {
	content := []byte("hello")

	d, _ := ioutil.TempDir("", "")
	defer os.RemoveAll(d)
	cache := NewDiskFileCache(d)

	h := crypto.MD5.New()
	h.Write(content)
	checksum := h.Sum()

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
