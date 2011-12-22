package cba

import (
	"bytes"
	"io"
	"io/ioutil"
	"testing"
//	"github.com/hanwen/go-fuse/fuse"
	"os"
	"syscall"
)

type netTestCase struct {
	tmp string
	server, clientCache *ContentCache
	sockS, sockC io.ReadWriteCloser
	client *Client
}

// TODO - cut & paste.
func unixSocketpair() (l *os.File, r *os.File, err error) {
	fd, err := syscall.Socketpair(syscall.AF_UNIX, syscall.SOCK_STREAM, 0)

	if err != nil {
		return nil, nil, os.NewSyscallError("socketpair",
			err.(syscall.Errno))
	}
	l = os.NewFile(fd[0], "socketpair-half1")
	r = os.NewFile(fd[1], "socketpair-half2")
	return
}

func (tc *netTestCase) Clean() {
	tc.sockS.Close()
	tc.sockC.Close()
	os.RemoveAll(tc.tmp)
}

func newNetTestCase(t *testing.T, cache bool) *netTestCase {
	me := &netTestCase{}
	me.tmp, _ = ioutil.TempDir("", "term-cba")
	
	optS := ContentCacheOptions{
		Dir: me.tmp + "/server",
	}
	if cache {
		optS.MemCount = 100
	}
	me.server = NewContentCache(&optS)
	
	optC := optS
	optC.Dir = me.tmp + "/client"
	me.clientCache = NewContentCache(&optC)
	var err error
	me.sockS, me.sockC, err = unixSocketpair()
	if err != nil {
		t.Fatalf("unixSocketpair: %v", err)
	}

	go me.server.ServeConn(me.sockS)
	me.client = me.clientCache.NewClient(me.sockC)
	return me 
}


func runTestNet(t *testing.T, cache bool) {
	tc := newNetTestCase(t, cache)
	defer tc.Clean()
	
	b := bytes.NewBufferString("hello")
	l := b.Len()
	hash := tc.server.SaveStream(b, int64(l))

	different := hash[1:] + "x"
	if success, err := tc.client.Fetch(different); success || err != nil {
		t.Errorf("non-existent fetch should return false without error: %v %v", success, err)
	}
	
	if success, err := tc.client.Fetch(hash); !success || err != nil {
		t.Fatalf("unexpected error: Fetch: %v,%v", success, err)
	}
	
	if !tc.clientCache.HasHash(hash) {
		t.Errorf("after fetch, the hash should be there")
	}

	tc.sockC.Close()
	if success, err := tc.client.Fetch(different); success || err == nil {
		t.Errorf("after close, fetch should return error: succ=%v", success)
	}
}

func TestNet(t *testing.T) {
	runTestNet(t, false)
}

func TestNetCache(t *testing.T) {
	runTestNet(t, true)
}

