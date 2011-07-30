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

func TestEscapeRegexp(t *testing.T) {
	s := EscapeRegexp("a+b")
	if s != "a\\+b" {
		t.Error("mismatch", s)
	}
}


func TestDetectFiles(t *testing.T) {
	fs := DetectFiles("/src/foo", "gcc /src/foo/bar.cc -I/src/foo/baz")
	result := map[string]int{}
	for _, f := range fs {
		result[f] = 1
	}
	if len(result) != 2 {
		t.Error("length", result)
	}
	if result["/src/foo/bar.cc"] != 1 ||  result["/src/foo/baz"] != 1 {
		t.Error("not found", result)
	}
}
