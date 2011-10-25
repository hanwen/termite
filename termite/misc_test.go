package termite

import (
	"bytes"
	"log"
	"testing"
)

var _ = log.Println

func TestHasDirPrefix(t *testing.T) {
	if !HasDirPrefix("a/b", "a") {
		t.Errorf("HasDirPrefix(a/b, a) fail")
	}
	if HasDirPrefix("a/b", "ab") {
		t.Errorf("HasDirPrefix(a/b, ab) succeed")
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
	if result["/src/foo/bar.cc"] != 1 || result["/src/foo/baz"] != 1 {
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
		"echo 'x' \\ >> temp.sed",
	}
	for _, s := range fail {
		result := ParseCommand(s)
		if result != nil {
			t.Errorf("should fail: cmd=%#v, result=%#v", s, result)
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
			for i := range r {
				if r[i] != entry.res[i] {
					t.Errorf("component mismatch for %v comp %d got %v want %v",
						entry.cmd, i, r[i], entry.res[i])
				}
			}
		}
	}
}

func TestSavingCopy(t *testing.T) {
	content := make([]byte, _BUFSIZE+1)
	for i := range content {
		content[i] = 'y'
	}

	readFrom := bytes.NewBuffer(content)
	writeTo := &bytes.Buffer{}
	content, err := SavingCopy(writeTo, readFrom, _BUFSIZE)
	if err != nil {
		t.Fatalf("SavingCopy failed with %v", err)
	}
	if content != nil {
		t.Errorf("Should drop contents for large copies.")
	}
}

func TestMakeUnescape(t *testing.T) {
	cases := []struct {
		in, out string
	}{
		{"abc\ndef", "abc\ndef"},
		{"abc\\\ndef", "abcdef"},
		{"abc\\\\\ndef", "abc\\\\\ndef"},
	}
	for i, c := range cases {
		got := MakeUnescape(c.in)
		if c.out != got {
			t.Errorf("%d: MakeUnescape(%q) = %q != %q", i, c.in, got, c.out)
		}
	}
}
