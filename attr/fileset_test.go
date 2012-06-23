package attr

import (
	"syscall"
	"testing"

	"github.com/hanwen/go-fuse/fuse"
)

func TestFileSet(t *testing.T) {
	fs := []*FileAttr{
		{Path: "b",
			Attr: &fuse.Attr{
				Mode: syscall.S_IFREG | 0644,
			},
		},
		{Path: "b"},
		{Path: "a"},
	}

	fset := FileSet{fs}
	fset.Sort()
	fs = fset.Files
	if !fs[0].Deletion() || fs[1].Path != "a" || fs[2].Path != "b" {
		t.Fatalf("incorrect sort order: %v", fs)
	}
}
