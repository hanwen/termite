package termite

import (
	"bytes"
	"testing"
)

func TestLocalDecider(t *testing.T) {
	str := ("# ignored\n" +
		"// ignored too\n" +
		"[{\"Regexp\": \".*foo\", \"Local\": false},\n " +
		"{\"Regexp\": \".*bar\", \"Local\": true}]")
	buf := bytes.NewBufferString(str)
	l := newLocalDecider(buf)

	r := l.ShouldRunLocally("xfoo")
	if r.Local != false {
		t.Error("-xfoo: expect false")
	}
	buf = bytes.NewBufferString(str)
	r = l.ShouldRunLocally("bar")
	if r.Local != true {
		t.Error("bar: expect true")
	}
}
