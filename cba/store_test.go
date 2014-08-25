package cba

import (
	"bytes"
	"crypto"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	md5pkg "crypto/md5"
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
	store   *Store
	options *StoreOptions
}

func newCcTestCase() *ccTestCase {
	d, _ := ioutil.TempDir("", "term-cc")
	opts := &StoreOptions{
		Dir: d,
	}
	store := NewStore(opts, nil)

	return &ccTestCase{d, store, opts}
}

func (me *ccTestCase) Clean() {
	os.RemoveAll(me.dir)
}

func TestHashWriter(t *testing.T) {
	tc := newCcTestCase()
	defer tc.Clean()

	content := []byte("hello")

	h := tc.store.NewHashWriter()
	h.Write(content)
	h.Close()

	want := string(md5(content))

	saved := h.Sum()
	if saved != want {
		t.Fatalf("mismatch got %x want %x", saved, want)
	}
	if !tc.store.Has(want) {
		t.Fatal("TestHashWriter: store does not have path")
	}
}

func TestStore(t *testing.T) {
	tc := newCcTestCase()
	defer tc.Clean()
	content := []byte("hello")

	checksum := string(md5(content))

	f, _ := ioutil.TempFile("", "")
	f.Write(content)
	f.Close()

	savedSum := tc.store.SavePath(f.Name())
	if savedSum != checksum {
		t.Fatalf("mismatch got %x want %x", savedSum, checksum)
	}
	if !tc.store.Has(checksum) {
		t.Fatal("path gone")
	}
}

func TestStoreDestructiveSave(t *testing.T) {
	tc := newCcTestCase()
	defer tc.Clean()
	d := tc.dir

	content := []byte("hello")
	fn := d + "/test"
	err := ioutil.WriteFile(fn, content, 0644)
	if err != nil {
		t.Error(err)
	}

	saved, err := tc.store.DestructiveSavePath(fn)
	check(err)
	if string(saved) != string(md5(content)) {
		t.Error("mismatch")
	}

	if !tc.store.Has(md5(content)) {
		t.Error("fail")
	}

	// Again.
	err = ioutil.WriteFile(fn, content, 0644)
	if err != nil {
		t.Error(err)
	}

	saved, err = tc.store.DestructiveSavePath(fn)
	check(err)
	want := md5(content)
	if saved == "" || saved != want {
		t.Error("mismatch got %x want %x", saved, want)
	}
	if fi, _ := os.Lstat(fn); fi != nil {
		t.Error("should have disappeared", fi)
	}
}

func TestStoreStream(t *testing.T) {
	tc := newCcTestCase()
	defer tc.Clean()
	content := []byte("hello")

	h := crypto.MD5.New()
	h.Write(content)
	checksum := string(h.Sum(nil))
	savedSum := tc.store.Save(content)
	got := string(savedSum)
	want := string(md5(content))
	if got != want {
		t.Fatalf("mismatch %x %x", got, want)
	}
	if !tc.store.Has(checksum) {
		t.Fatal("path gone")
	}

	data, err := ioutil.ReadFile(tc.store.Path(checksum))
	if err != nil {
		t.Fatal(err)
	}

	if bytes.Compare(data, content) != 0 {
		t.Error("compare.")
	}
}

func TestStoreSave(t *testing.T) {
	tc := newCcTestCase()
	defer tc.Clean()

	sz := 1025
	content := make([]byte, sz)
	for i := range content {
		content[i] = 'x'
	}

	hash := tc.store.Save(content)

	if !tc.store.Has(hash) {
		t.Errorf("should have key %x", hash)
	}
}

func TestHashPath(t *testing.T) {
	h := string([]byte{1, 2, 3, 20, 255})
	dir, err := ioutil.TempDir("", "cba-test")
	if err != nil {
		t.Fatalf("TempDir: %v", err)
	}

	hex := fmt.Sprintf("%x", h)
	want := filepath.Join(dir, hex[:2], hex[2:])

	got := HashPath(dir, h)
	if want != got {
		t.Errorf("got %q want %q", got, want)
	}
}
