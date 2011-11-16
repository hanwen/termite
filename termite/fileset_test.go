package termite

import (
	"os"
	"syscall"
	"testing"
)

func TestFileSet(t *testing.T) {
	fs := []*FileAttr{
		&FileAttr{Path: "b",
			FileInfo: &os.FileInfo{
				Mode: syscall.S_IFREG | 0644,
			},
		},
		&FileAttr{Path: "b"},
		&FileAttr{Path: "a"},
	}

	fset := FileSet{fs}
	fset.Sort()
	fs = fset.Files
	if !fs[0].Deletion() || fs[1].Path != "a" || fs[2].Path != "b" {
		t.Fatalf("incorrect sort order: %v", fs)
	}
}
