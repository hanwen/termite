package termite

import (
	"os"
	"io/ioutil"
	"testing"
)

func TestFileSaver(t *testing.T) {
	dir, _ := ioutil.TempDir("", "tasktest")
	defer os.RemoveAll(dir)
	rw := dir + "/tasktest/rw"
	os.MkdirAll(rw+"/"+_DELETIONS, 0755)
	os.MkdirAll(rw+"/dir", 0755)

	content := []byte("hello")
	ioutil.WriteFile(rw+"/dir/file1", content, 0644)
	ioutil.WriteFile(rw+"/"+_DELETIONS+"/entry",
		[]byte("file2"), 0644)
	ioutil.WriteFile(rw+"/ignore",
		[]byte("xxx"), 0644)
	os.Symlink("orig", rw+"/dir/symlink")

	s := fileSaver{
		rwDir:  rw,
		prefix: "/dir",
		cache:  NewContentCache(dir + "/cache"),
	}

	s.reapBackingStore()
	if s.err != nil {
		t.Fatal("scan", s.err)
	}

	byPath := make(map[string]*FileAttr)
	for _, f := range s.files {
		byPath[f.Path] = f
	}
	f, ok := byPath["/dir"]
	if !ok || !f.FileInfo.IsDirectory() {
		t.Error("/dir", f)
	}
	f, ok = byPath["/dir/file1"]
	if !ok || !f.FileInfo.IsRegular() || string(f.Hash) != string(md5(content)) {
		t.Fatal("/dir/file1", f)
	}

	f, ok = byPath["/dir/symlink"]
	if !ok || f.Link != "orig" {
		t.Fatal("/dir/file1", f)
	}

	_, ok = byPath["/ignore"]
	if ok {
		t.Fatal("/ignore", f)
	}
}
