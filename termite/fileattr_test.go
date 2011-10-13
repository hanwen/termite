package termite

import (
	"io/ioutil"
	"os"
	"syscall"
	"testing"
)

func TestFileAttrReadFrom(t *testing.T) {
	dir, _ := ioutil.TempDir("", "termite")
	ioutil.WriteFile(dir + "/file.txt", []byte{42}, 0644)

	attr := FileAttr{FileInfo: &os.FileInfo{Mode: syscall.S_IFDIR}}
	attr.ReadFromFs(dir)
	if attr.NameModeMap == nil {
		t.Fatalf("should have NameModeMap: %v", attr)
	}

	if attr.NameModeMap["file.txt"].IsRegular() {
		t.Fatalf("unexpected mode: %v",  attr.NameModeMap["file.txt"])
	}
}
