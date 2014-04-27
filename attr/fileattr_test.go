package attr

import (
	"crypto"
	"crypto/md5"
	"io/ioutil"
	"syscall"
	"testing"

	"github.com/hanwen/go-fuse/fuse"
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
	if m&syscall.S_IFREG == 0 {
		t.Fatalf("unexpected mode: %o, want %o", m, syscall.S_IFREG)
	}
}

func TestFileModeString(t *testing.T) {
	got := FileMode(syscall.S_IFDIR | 0755).String()
	want := "d:755"
	if got != want {
		t.Errorf("got %q want %q", got, want)
	}
}

func TestEncode(t *testing.T) {
	e := EncodedAttr{}

	a := fuse.Attr{
		Mode:      0x11111111,
		Nlink:     0x22222222,
		Size:      0x333333333333,
		Ino:       0x444444444444,
		Mtime:     0x55555555,
		Mtimensec: 123456789,
	}

	e.FromAttr(&a)
	if a.Size != e.Size {
		t.Fatalf("EncodedAttr.FromAttr", e, a)
	}

	h := "abc"
	encoded := e.Encode(h)

	dec := EncodedAttr{}
	decHash := dec.Decode(encoded)
	if string(decHash) != h {
		t.Fatalf("Decoded hash: %q != %q", decHash, h)
	}

	if !dec.Eq(&e) {
		t.Fatalf("decoded EncodedAttr got %#v != want %#v", dec, e)
	}
}
