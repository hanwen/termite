package termite

import (
	"io/ioutil"
	"testing"
)

func TestListFilesRecursively(t *testing.T) {
	dir, _ := ioutil.TempDir("", "listfiles")
	ioutil.WriteFile(dir+"/foo", []byte{42}, 0644)
	entries := ListFilesRecursively(dir)
	if len(entries) != 2 {
		t.Errorf("expect 2 entries %#v", entries)
	}
	attr, ok := entries[dir]
	if !ok || !attr.IsDirectory() {
		t.Errorf("unexpected entry for 'dir': %v, %#v", ok, entries[dir])
	}

	attr, ok = entries[dir+"/foo"]
	if !ok || !attr.IsRegular() {
		t.Errorf("unexpected entry for 'dir/foo': %v, %#v", ok, entries[dir])
	}
}
