package rpcfs

import (
//	"os"
	"crypto"
	"testing"
	"io/ioutil"
)

func TestDiskCache(t *testing.T) {
	content := []byte("hello")

	d, _ := ioutil.TempDir("", "")
	
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
