package termite

import (
	"io/ioutil"
	"testing"
)

func TestCopyFile(t *testing.T) {
	err := ioutil.WriteFile("src.txt", []byte("hello"), 0644)
	if err != nil {
		t.Error(err)
	}
	err = CopyFile("dest.txt", "src.txt", 0755)
	if err != nil {
		t.Error(err)
	}

	c, err := ioutil.ReadFile("dest.txt")
	if err != nil {
		t.Error(err)
	}
	if string(c) != "hello" {
		t.Error("mismatch", string(c))
	}
}
