package cba

import (
	"bytes"
	"io"
	"io/ioutil"
	"os"
	"syscall"
	"testing"
)

type netTestCase struct {
	tmp                 string
	server, clientStore *Store
	sockS, sockC        io.ReadWriteCloser
	client              *Client
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

	optS := StoreOptions{
		Dir: me.tmp + "/server",
	}
	if cache {
		optS.MemCount = 100
	}
	me.server = NewStore(&optS)

	optC := optS
	optC.Dir = me.tmp + "/client"
	me.clientStore = NewStore(&optC)
	var err error
	me.sockS, me.sockC, err = unixSocketpair()
	if err != nil {
		t.Fatalf("unixSocketpair: %v", err)
	}

	go me.server.ServeConn(me.sockS)
	me.client = me.clientStore.NewClient(me.sockC)
	return me
}

func runTestNet(t *testing.T, store bool) {
	tc := newNetTestCase(t, store)
	defer tc.Clean()

	b := bytes.NewBufferString("hello")
	l := b.Len()
	hash := tc.server.SaveStream(b, int64(l))

	different := hash[1:] + "x"
	if success, err := tc.client.Fetch(different, 1024); success || err != nil {
		t.Errorf("non-existent fetch should return false without error: %v %v", success, err)
	}

	if success, err := tc.client.Fetch(hash, 1024); !success || err != nil {
		t.Fatalf("unexpected error: Fetch: %v,%v", success, err)
	}

	if !tc.clientStore.HasHash(hash) {
		t.Errorf("after fetch, the hash should be there")
	}

	tc.sockC.Close()
	if success, err := tc.client.Fetch(different, 1024); success || err == nil {
		t.Errorf("after close, fetch should return error: succ=%v", success)
	}
}

func TestNet(t *testing.T) {
	runTestNet(t, false)
}

func TestNetCache(t *testing.T) {
	runTestNet(t, true)
}

func TestNetLargeFile(t *testing.T) {
	b := make([]byte, 257 * 1024)
	for i, _ := range b {
		b[i] = byte(i)
	}

	tc := newNetTestCase(t, false)
	defer tc.Clean()

	hash := tc.server.Save(b)

	tc.client.Fetch(hash, int64(len(b)))
	if !tc.clientStore.HasHash(hash) {
		t.Errorf("after fetch, the hash should be there")
	}
}
