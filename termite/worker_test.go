package termite

import (
	"fmt"
	"http"
	"io/ioutil"
	"log"
	"os"
	"rand"
	"rpc"
	"testing"
	"time"
)

type testCase struct {
	worker *WorkerDaemon
	master *Master
	coordinator *Coordinator
	secret []byte
	tmp    string
	socket string
	coordinatorPort int
	workerPort int
}

func NewTestCase() *testCase {
	me := new(testCase)
	me.secret = RandomBytes(20)
	me.tmp, _ = ioutil.TempDir("", "")

	workerTmp := me.tmp + "/worker-tmp"
	os.Mkdir(workerTmp, 0700)
	me.worker = NewWorkerDaemon(me.secret, workerTmp,
		me.tmp+"/worker-cache", 1)

	// TODO - pick unused port
	me.coordinatorPort = int(rand.Int31n(60000) + 1024)
	c := NewCoordinator()
	rpc.Register(c)
	rpc.HandleHTTP()
	go c.PeriodicCheck()

	coordinatorAddr := fmt.Sprintf(":%d", me.coordinatorPort)
	go http.ListenAndServe(coordinatorAddr, nil)
	// TODO - can we do without the sleeps?
	time.Sleep(0.1e9) // wait for daemon to start up
	
	me.workerPort = int(rand.Int31n(60000) + 1024)
	go me.worker.RunWorkerServer(me.workerPort, coordinatorAddr)

	// wait worker to be registered on coordinator.
	time.Sleep(0.1e9)

	masterCache := NewContentCache(me.tmp + "/master-cache")
	me.master = NewMaster(
		masterCache, coordinatorAddr,
		[]string{},
		me.secret, []string{}, 1)

	me.master.SetKeepAlive(1)
	me.socket = me.tmp + "/master-socket"
	go me.master.Start(me.socket)
	
	wd := me.tmp + "/wd"
	os.MkdirAll(wd, 0755)
	return me
}

func (me *testCase) Clean() {
	me.master.mirrors.dropConnections()
	time.Sleep(0.1e9)
	os.RemoveAll(me.tmp)
}

// Simple end-to-end test.  It skips the chroot, but should give a
// basic assurance that things work as expected.
func TestBasic(t *testing.T) {
	if os.Geteuid() == 0 {
		log.Println("This test should not run as root")
		return
	}

	tc := NewTestCase()
	defer tc.Clean()
	
	req := WorkRequest{
		StdinId: ConnectionId(),
		Binary:  "/usr/bin/tee",
		Argv:    []string{"/usr/bin/tee", "output.txt"},
		Env:     os.Environ(),

		// Will not be filtered, since /tmp/foo is more
		// specific than /tmp
		Dir: tc.tmp + "/wd",
	}

	// TODO - should separate dial/listen in the daemons?
	time.Sleep(0.1e9) // wait for all daemons to start up
	stdinConn := OpenSocketConnection(tc.socket, req.StdinId)
	go func() {
		stdinConn.Write([]byte("hello"))
		stdinConn.Close()
	}()

	rpcConn := OpenSocketConnection(tc.socket, RPC_CHANNEL)
	client := rpc.NewClient(rpcConn)

	rep := WorkReply{}
	err := client.Call("LocalMaster.Run", &req, &rep)
	if err != nil {
		log.Fatal("LocalMaster.Run: ", err)
	}

	content, err := ioutil.ReadFile(tc.tmp + "/wd/output.txt")
	if err != nil {
		t.Error(err)
	}
	if string(content) != "hello" {
		t.Error("content:", content)
	}

	req = WorkRequest{
		Binary:  "/bin/rm",
		Argv:    []string{"/bin/rm", "output.txt"},
		Env:     os.Environ(),
		Dir:     tc.tmp + "/wd",
	}

	rep = WorkReply{}
	err = client.Call("LocalMaster.Run", &req, &rep)
	if err != nil {
		t.Fatal("LocalMaster.Run: ", err)
	}
	if fi, _ := os.Lstat(tc.tmp + "/wd/output.txt"); fi != nil {
		t.Error("file should have been deleted", fi)
	}

	// TODO - test mkdir dir && touch dir/foo.txt, rm -rf dir.
	
	// Test keepalive.
	time.Sleep(2e9)

	statusReq := &StatusRequest{}
	statusRep := &StatusReply{}
	tc.worker.Status(statusReq, statusRep)
	if statusRep.Processes != 0 {
		t.Error("Processes still alive.")
	}
}
