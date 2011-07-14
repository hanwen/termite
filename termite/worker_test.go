package termite

import (
	"fmt"
	"io/ioutil"
	"os"
	"rand"
	"testing"
	"rpc"
	"log"
)

func TestBasic(t *testing.T) {
	secret := RandomBytes(20)
	tmp, _ := ioutil.TempDir("", "")

	workerTmp := tmp + "/worker-tmp"
	os.Mkdir(workerTmp, 0700)
	worker := NewWorkerDaemon(secret, workerTmp,
		tmp + "/worker-cache", 1)

	// TODO - pick unused port
	workerPort := int(rand.Int31n(60000) + 1024)

	// TODO - test coordinator too.
	go worker.RunWorkerServer(workerPort, "")

	masterCache := NewContentCache(tmp + "/master-cache")
	host, _ := os.Hostname()
	master := NewMaster(
		masterCache, "",
		[]string{fmt.Sprintf("%s:%d", host, workerPort)},
		secret, []string{}, 1)

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
		Dir:     tmp + "/wd",
	}

	stdinConn := OpenSocketConnection(socket, req.StdinId)
	go func(){
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
	

}
