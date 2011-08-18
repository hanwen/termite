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

	local, _ := l.ShouldRunLocally("xfoo")
	if local != false {
		t.Error("-xfoo: expect false")
	}
	buf = bytes.NewBufferString(str)
	local, _ = l.ShouldRunLocally("bar")
	if local != true {
		t.Error("bar: expect true")
	}
}
