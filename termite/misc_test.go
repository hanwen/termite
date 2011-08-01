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

func TestParseCommand(t *testing.T) {
	fail := []string{
		"echo hoi;",
		"echo \"${hoi}\"",
		"a && b",
		"a || b",
		"echo a*b",
	}
	for _, s := range fail {
		result := ParseCommand(s)
		if result != nil {
			t.Error("should fail", s, result)
		}
	}

	type Succ struct {
		cmd string
		res []string
	}

	succ := []Succ{
		Succ{"echo \"a'b\"", []string{"echo", "a'b"}},
		Succ{"\"a'b\"", []string{"a'b"}},
		Succ{"a\\ b", []string{"a b"}},
		Succ{"a'x y'b", []string{"ax yb"}},
		Succ{"echo \"a[]<>*&;;\"", []string{"echo", "a[]<>*&;;"}},
		Succ{"a   b", []string{"a", "b"}},
		Succ{"a\\$b", []string{"a$b"}},
	}
	for _, entry := range succ {
		r := ParseCommand(entry.cmd)
		if len(r) != len(entry.res) {
			t.Error("len mismatch", r, entry)
		} else {
			for i, _ := range r {
				if r[i] != entry.res[i] {
					t.Errorf("component mismatch for %v comp %d got %v want %v",
						entry.cmd, i, r[i], entry.res[i])
				}
			}
		}
	}
}	
