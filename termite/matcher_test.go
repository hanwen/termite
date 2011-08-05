package termite

import (
	"bytes"
	"testing"
	)

func TestMatchAgainst(t *testing.T) {
	str := "-.*foo\n" + "bar\n"

	buf := bytes.NewBufferString(str)
	if MatchAgainst(buf, "xfoo") != false {
		t.Error("-xfoo: expect false");
	}
	buf = bytes.NewBufferString(str)
	if MatchAgainst(buf, "bar") != true {
		t.Error("bar: expect false");
	}
}
