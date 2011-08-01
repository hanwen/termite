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

// Simple end-to-end test.  It skips the chroot, but should give a
// basic assurance that things work as expected.
func TestBasic(t *testing.T) {
	if os.Geteuid() == 0 {
		log.Println("This test should not run as root")
		return
	}

	secret := RandomBytes(20)
	tmp, _ := ioutil.TempDir("", "")

	workerTmp := tmp + "/worker-tmp"
	os.Mkdir(workerTmp, 0700)
	worker := NewWorkerDaemon(secret, workerTmp,
		tmp+"/worker-cache", 1)

	// TODO - pick unused port
	coordinatorPort := int(rand.Int31n(60000) + 1024)
	c := NewCoordinator()
	rpc.Register(c)
	rpc.HandleHTTP()
	go c.PeriodicCheck()

	coordinatorAddr := fmt.Sprintf(":%d", coordinatorPort)
	go http.ListenAndServe(coordinatorAddr, nil)

	// TODO - can we do without the sleeps?
	time.Sleep(0.1e9) // wait for daemon to start up

	workerPort := int(rand.Int31n(60000) + 1024)
	go worker.RunWorkerServer(workerPort, coordinatorAddr)

	// wait worker to be registered on coordinator.
	time.Sleep(0.1e9)

	masterCache := NewContentCache(tmp + "/master-cache")
	master := NewMaster(
		masterCache, coordinatorAddr,
		[]string{},
		secret, []string{}, 1)

	master.SetKeepAlive(1)
	socket := tmp + "/master-socket"
	go master.Start(socket)
	wd := tmp + "/wd"
	os.MkdirAll(wd, 0755)

	req := WorkRequest{
		StdinId: ConnectionId(),
		Binary:  "/usr/bin/tee",
		Argv:    []string{"/usr/bin/tee", "output.txt"},
		Env:     os.Environ(),

		// Will not be filtered, since /tmp/foo is more
		// specific than /tmp
		Dir: tmp + "/wd",
	}

	// TODO - should separate dial/listen in the daemons?
	time.Sleep(0.1e9) // wait for all daemons to start up
	stdinConn := OpenSocketConnection(socket, req.StdinId)
	go func() {
		stdinConn.Write([]byte("hello"))
		stdinConn.Close()
	}()

	rpcConn := OpenSocketConnection(socket, RPC_CHANNEL)
	client := rpc.NewClient(rpcConn)

	rep := WorkReply{}
	err := client.Call("LocalMaster.Run", &req, &rep)
	if err != nil {
		log.Fatal("LocalMaster.Run: ", err)
	}

	content, err := ioutil.ReadFile(tmp + "/wd/output.txt")
	if err != nil {
		t.Error(err)
	}
	if string(content) != "hello" {
		t.Error("content:", content)
	}

	req = WorkRequest{
		StdinId: ConnectionId(),
		Binary:  "/bin/rm",
		Argv:    []string{"/bin/rm", "output.txt"},
		Env:     os.Environ(),
		Dir:     tmp + "/wd",
	}
	stdinConn = OpenSocketConnection(socket, req.StdinId)

	rep = WorkReply{}
	err = client.Call("LocalMaster.Run", &req, &rep)
	if err != nil {
		t.Fatal("LocalMaster.Run: ", err)
	}
	if fi, _ := os.Lstat(tmp + "/wd/output.txt"); fi != nil {
		t.Error("file should have been deleted", fi)
	}

	// TODO - test mkdir dir && touch dir/foo.txt, rm -rf dir.
	
	// Test keepalive.
	time.Sleep(2e9)

	statusReq := &StatusRequest{}
	statusRep := &StatusReply{}
	worker.Status(statusReq, statusRep)
	if statusRep.Processes != 0 {
		t.Error("Processes still alive.")
	}
}
