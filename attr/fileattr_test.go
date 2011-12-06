package attr

import (
	"crypto"
	"crypto/md5"
	"github.com/hanwen/go-fuse/fuse"	
	"io/ioutil"
	"syscall"
	"testing"
)

var _ = md5.New

func TestFileAttrReadFrom(t *testing.T) {
	dir, _ := ioutil.TempDir("", "termite")
	ioutil.WriteFile(dir+"/file.txt", []byte{42}, 0644)

	attr := FileAttr{Attr: &fuse.Attr{Mode: syscall.S_IFDIR}}
	attr.ReadFromFs(dir, crypto.MD5)
	if attr.NameModeMap == nil {
		t.Fatalf("should have NameModeMap: %v", attr)
	}

	m := attr.NameModeMap["file.txt"]
	if !m.IsRegular() {
		t.Fatalf("unexpected mode: %o, want IsRegular()", m)
	}
}
