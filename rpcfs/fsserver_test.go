package rpcfs

import (
	"bytes"
	"fmt"
	"github.com/hanwen/go-fuse/fuse"
	"log"
	"strings"
	"testing"
)
var _ = fmt.Println
var _ = log.Println
func TestReadChunks(t *testing.T) {
	content := strings.Repeat("blabla", 2)
	buf := bytes.NewBufferString(content)
	if buf.Len() != len(content) {
		t.Fatal("huh")
	}

	ch, err := ReadChunks(buf, 10)
	if err != nil {
		t.Fatal("ReadChunks err", err)
	}

	if len(ch) != 2 || len(ch[0]) + len(ch[1]) != len(content) {
		t.Fatal("len mismatch", ch)
	}

	back := string(ch[0]) + string(ch[1])
	if back != content {
		t.Fatal("content mismatch", back, content)
	}

	cf := NewChunkedFile(ch)

	
	bp := fuse.NewGcBufferPool()
	out, code := cf.Read(&fuse.ReadIn{Size: 1024}, bp)
	if !code.Ok() {
		t.Fatal("read err", code)
	}

	if string(out) != content {
		t.Fatal("ChunkedFile mismatch", string(out))
	}
}
