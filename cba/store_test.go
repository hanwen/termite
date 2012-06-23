package cba

import (
	"bytes"
	"crypto"
	md5pkg "crypto/md5"
	"fmt"
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
	store   *Store
	options *StoreOptions
}

func newCcTestCase() *ccTestCase {
	d, _ := ioutil.TempDir("", "term-cc")
	opts := &StoreOptions{
		Dir:        d,
		MemCount:   10,
		MemMaxSize: 1024,
	}
	store := NewStore(opts)

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
	if !tc.store.HasHash(want) {
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
	if !tc.store.HasHash(checksum) {
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

	if !tc.store.HasHash(md5(content)) {
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
	if !tc.store.HasHash(checksum) {
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

func TestStoreStreamReturnContent(t *testing.T) {
	tc := newCcTestCase()
	defer tc.Clean()
	content := make([]byte, tc.options.MemMaxSize-1)
	for i := range content {
		content[i] = 'x'
	}

	hash := tc.store.Save(content)

	if !tc.store.inMemoryCache.Has(hash) {
		t.Errorf("should have key %x", hash)
	}

	content = make([]byte, tc.options.MemMaxSize+1)
	for i := range content {
		content[i] = 'y'
	}

	f, _ := ioutil.TempFile("", "term-cc")
	err := ioutil.WriteFile(f.Name(), content, 0644)
	check(err)
	hash = tc.store.SavePath(f.Name())
	if tc.store.inMemoryCache.Has(hash) {
		t.Errorf("should not have key %x %v", hash, tc.store.inMemoryCache.Get(hash))
	}
}

func TestHashPath(t *testing.T) {
	h := string([]byte{1, 2, 3, 20, 255})
	hex := fmt.Sprintf("%x", h)
	want := "d/" + hex[:2] + "/" + hex[2:]

	got := HashPath("d", h)
	if want != got {
		t.Errorf("got %q want %q", got, want)
	}
}
