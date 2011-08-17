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

	if l.shouldRunLocally("xfoo") != false {
		t.Error("-xfoo: expect false")
	}
	buf = bytes.NewBufferString(str)
	if l.shouldRunLocally("bar") != true {
		t.Error("bar: expect true")
	}
}
