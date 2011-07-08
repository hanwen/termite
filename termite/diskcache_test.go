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
	
	cache := NewDiskFileCache(d)

	h := crypto.MD5.New()
	h.Write(content)
	checksum := h.Sum()

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
	
	h := crypto.MD5.New()
	h.Write(content)
	checksum := h.Sum()
	
	d, _ := ioutil.TempDir("", "")
	defer os.RemoveAll(d)
	cache := NewDiskFileCache(d)

	fn := d + "/test"
	err := ioutil.WriteFile(fn, content, 0644)
	if err != nil { t.Error(err) }

	saved := cache.DestructiveSavePath(fn)
	if string(saved) != string(checksum) {
		t.Error("mismatch")
	}

	if !cache.HasHash(checksum) {
		t.Error("fail")
	}

	err = ioutil.WriteFile(fn, content, 0644)
	if err != nil { t.Error(err) }

	saved = cache.DestructiveSavePath(fn)
	if saved == nil || string(saved) != string(checksum) {
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
	if string(savedSum) != string(checksum) {
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
