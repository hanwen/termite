package attr

import (
	"github.com/hanwen/go-fuse/fuse"
	"reflect"
	"testing"
)

func TestRoundtrip(t *testing.T) {
	a := FileAttr{
	Link: "Link",
	Hash: "abcd",
	Path: "Path",
	Attr: &fuse.Attr{Mode: 0100755, Size: 22},
	}
	
	b, err := a.Encode()
	if err != nil {
		t.Fatal("Encode:", err)
	}
	c := FileAttr{}
	err = c.Decode(b)
	if err != nil {
		t.Fatal("Decode:", err)
	}
	if !reflect.DeepEqual(a.Attr, c.Attr) {
		t.Fatalf("DeepEqual(attr): got %v want %v", c.Attr, a.Attr)
	}

	// TODO - why can't use DeepEqual?
	if a.Hash != c.Hash || a.Link!=c.Link || a.Path != c.Path {
		t.Fatalf("FileAttr mismatch: got %v want %v", c, a)
	}
}
