package termite

import (
	"io/ioutil"
	"os"
	"syscall"
	"testing"
)

func TestFileAttrReadFrom(t *testing.T) {
	dir, _ := ioutil.TempDir("", "termite")
	ioutil.WriteFile(dir+"/file.txt", []byte{42}, 0644)

	attr := FileAttr{FileInfo: &os.FileInfo{Mode: syscall.S_IFDIR}}
	attr.ReadFromFs(dir)
	if attr.NameModeMap == nil {
		t.Fatalf("should have NameModeMap: %v", attr)
	}

	m := attr.NameModeMap["file.txt"]
	if !m.IsRegular() {
		t.Fatalf("unexpected mode: %o, want IsRegular()", m)
	}
}

func TestFileMode(t *testing.T) {
	sock := FileMode(syscall.S_IFSOCK)
	if sock.IsDirectory() {
		t.Error("Socket should not be directory")
	}
}
