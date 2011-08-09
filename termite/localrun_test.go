package termite

import (
	"bytes"
	"testing"
)

func TestLocalDecider(t *testing.T) {
	str := "-.*foo\n" + "bar\n"

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
