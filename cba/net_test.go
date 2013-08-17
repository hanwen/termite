package cba

import (
	"bytes"
	"io"
	"io/ioutil"
	"net"
	"os"
	"testing"

	"github.com/hanwen/go-fuse/splice"
)

type netTestCase struct {
	tester              *testing.T
	tmp                 string
	server, clientStore *Store
	sockS, sockC        io.ReadWriteCloser
	client              *Client
	startSplices        int
}

func (tc *netTestCase) Clean() {
	tc.sockS.Close()
	tc.sockC.Close()
	os.RemoveAll(tc.tmp)
	if tc.startSplices != splice.Used() {
		tc.tester.Fatalf("Splice leak before %d after %d",
			tc.startSplices, splice.Used())
	}
}

func newNetTestCase(t *testing.T) *netTestCase {
	me := &netTestCase{}
	me.tester = t
	me.startSplices = splice.Used()
	me.tmp, _ = ioutil.TempDir("", "term-cba")

	optS := StoreOptions{
		Dir: me.tmp + "/server",
	}
	me.server = NewStore(&optS)

	optC := optS
	optC.Dir = me.tmp + "/client"
	me.clientStore = NewStore(&optC)
	var err error
	me.sockS, me.sockC = net.Pipe()
	if err != nil {
		t.Fatalf("unixSocketpair: %v", err)
	}

	go me.server.ServeConn(me.sockS)
	me.client = me.clientStore.NewClient(me.sockC)
	return me
}

func TestNet(t *testing.T) {
	tc := newNetTestCase(t)
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

	if !tc.clientStore.Has(hash) {
		t.Errorf("after fetch, the hash should be there")
	}

	tc.sockC.Close()
	if success, err := tc.client.Fetch(different, 1024); success || err == nil {
		t.Errorf("after close, fetch should return error: succ=%v", success)
	}
}

func TestNetLargeFile(t *testing.T) {
	b := make([]byte, 257*1024)
	for i, _ := range b {
		b[i] = byte(i)
	}

	tc := newNetTestCase(t)
	defer tc.Clean()

	hash := tc.server.Save(b)

	tc.client.Fetch(hash, int64(len(b)))
	if !tc.clientStore.Has(hash) {
		t.Errorf("after fetch, the hash should be there")
	}
}
