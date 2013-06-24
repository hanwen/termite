package fs

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/hanwen/go-fuse/fuse"
	"github.com/hanwen/go-fuse/fuse/nodefs"
)

func setupDevNullFs() (wd string, clean func()) {
	fs := NewDevFs()
	mountPoint, _ := ioutil.TempDir("", "termite")
	state, _, err := nodefs.MountFileSystem(mountPoint, fs, nil)
	if err != nil {
		panic(err)
	}

	state.SetDebug(fuse.VerboseTest())
	go state.Loop()
	return mountPoint, func() {
		state.Unmount()
		os.RemoveAll(mountPoint)
	}
}

func TestDevNullFs(t *testing.T) {
	wd, clean := setupDevNullFs()
	defer clean()

	err := ioutil.WriteFile(wd+"/null", []byte("ignored"), 0644)
	if err != nil {
		t.Error(err)
	}

	result, err := ioutil.ReadFile(wd + "/null")
	if err != nil {
		t.Error(err)
	}
	if len(result) > 0 {
		t.Error("Should have 0 length read.")
	}
}

func TestRandom(t *testing.T) {
	wd, clean := setupDevNullFs()
	defer clean()

	c, err := ioutil.ReadFile(wd + "/urandom")
	if err != nil {
		t.Error("random read failed", err)
	}
	if len(c) == 0 {
		t.Error("/dev/urandom returned nothing.")
	}
}
