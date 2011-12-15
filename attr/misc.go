package attr

import (
	"path/filepath"
	"strings"
)

// TODO - move into fuse
func SplitPath(name string) (dir, base string) {
	dir, base = filepath.Split(name)
	dir = strings.TrimRight(dir, "/")
	return dir, base
}
