package termite

import (
	"exec"
	"fmt"
	"github.com/hanwen/go-fuse/fuse"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"rand"
	"rpc"
	"strings"
	"sync"
	"testing"
	"time"
)

type testCase struct {
	worker          *WorkerDaemon
	master          *Master
	coordinator     *Coordinator
	secret          []byte
	tmp             string
	socket          string
	coordinatorPort int
	workerPort      int
	tester          *testing.T
	startFdCount    int
}

func (me *testCase) FindBin(name string) string {
	full, err := exec.LookPath(name)
	if err != nil {
		me.tester.Fatal("looking for binary:", err)
	}

	full, err = filepath.EvalSymlinks(full)
	if err != nil {
		me.tester.Fatal("EvalSymlinks:", err)
	}
	return full
}

func testEnv() []string {
	return []string{
		"PATH=/bin:/usr/bin",
		"USER=nobody",
	}
}

func NewTestCase(t *testing.T) *testCase {
	me := new(testCase)
	me.tester = t
	me.secret = RandomBytes(20)
	me.tmp, _ = ioutil.TempDir("", "")

	me.startFdCount = me.fdCount()
	workerTmp := me.tmp + "/worker-tmp"
	os.Mkdir(workerTmp, 0700)
	opts := WorkerOptions{
		Secret: me.secret,
		TempDir: workerTmp,
		CacheDir: me.tmp+"/worker-cache",
		Jobs: 1,
	}

	me.worker = NewWorkerDaemon(&opts)

	// TODO - pick unused port
	me.coordinatorPort = int(rand.Int31n(60000) + 1024)
	me.workerPort = int(rand.Int31n(60000) + 1024)

	coordinatorAddr := fmt.Sprintf(":%d", me.coordinatorPort)
	var wg sync.WaitGroup

	wg.Add(2)
	go func() {
		me.coordinator = NewCoordinator(me.secret)
		go me.coordinator.PeriodicCheck()
		go me.coordinator.ServeHTTP(me.coordinatorPort)
		wg.Done()
	}()
	go func() {
		masterCache := NewContentCache(me.tmp + "/master-cache")
		me.master = NewMaster(
			masterCache, coordinatorAddr,
			[]string{},
			me.secret, []string{}, 1)
		me.master.fileServer.excludePrivate = false
		me.master.SetKeepAlive(0.5, 0.5)
		me.socket = me.tmp + "/master-socket"
		go me.master.Start(me.socket)
		wg.Done()
	}()
	go me.worker.RunWorkerServer(me.workerPort, coordinatorAddr)

	wg.Wait()
	for me.coordinator.WorkerCount() == 0 {
		log.Println("reporting...")
		me.worker.report(coordinatorAddr, me.workerPort)
	}

	wd := me.tmp + "/wd"
	os.MkdirAll(wd, 0755)
	return me
}

func (me *testCase) fdCount() int {
	entries, err := ioutil.ReadDir("/proc/self/fd")
	if err != nil {
		me.tester.Fatal("ReadDir fd", err)
	}
	return len(entries)
}

func (me *testCase) Clean() {
	log.Println("cleaning up testcase.")
	me.master.mirrors.dropConnections()
	me.worker.Shutdown(nil, nil)
	me.coordinator.Shutdown()
	// TODO - should have explicit worker shutdown routine.
	time.Sleep(0.1e9)
	os.RemoveAll(me.tmp)

	// TODO - there are still some persistent leaks here.
	if false && me.fdCount() > me.startFdCount {
		me.tester.Errorf("Fd leak. Start: %d, end %d", me.startFdCount, me.fdCount())
		dir := "/proc/self/fd"
		entries, _ := ioutil.ReadDir(dir)
		for _, e := range entries {
			l, _  := os.Readlink(filepath.Join(dir, e.Name))
			log.Printf("%s -> %q", e.Name, l)
		}
	}
}

func (me *testCase) Run(req WorkRequest) (rep WorkReply) {
	rpcConn := OpenSocketConnection(me.socket, RPC_CHANNEL)
	client := rpc.NewClient(rpcConn)

	err := client.Call("LocalMaster.Run", &req, &rep)
	if err != nil {
		me.tester.Fatal("LocalMaster.Run: ", err)
	}
	client.Close()
	return rep
}

// Simple end-to-end test.  It skips the chroot, but should give a
// basic assurance that things work as expected.
func TestEndToEndBasic(t *testing.T) {
	if os.Geteuid() == 0 {
		log.Println("This test should not run as root")
		return
	}

	tc := NewTestCase(t)
	defer tc.Clean()

	req := WorkRequest{
		StdinId: ConnectionId(),
		Binary:  tc.FindBin("tee"),
		Argv:    []string{"tee", "output.txt"},
		Env:     testEnv(),

		// Will not be filtered, since /tmp/foo is more
		// specific than /tmp
		Dir: tc.tmp + "/wd",
	}

	// TODO - should separate dial/listen in the daemons?
	stdinConn := OpenSocketConnection(tc.socket, req.StdinId)
	go func() {
		stdinConn.Write([]byte("hello"))
		stdinConn.Close()
	}()

	tc.Run(req)
	content, err := ioutil.ReadFile(tc.tmp + "/wd/output.txt")
	if err != nil {
		t.Error(err)
	}
	if string(content) != "hello" {
		t.Error("content:", content)
	}

	tc.Run(WorkRequest{
		Binary: tc.FindBin("rm"),
		Argv:   []string{"rm", "output.txt"},
		Env:    testEnv(),
		Dir:    tc.tmp + "/wd",
	})

	if fi, _ := os.Lstat(tc.tmp + "/wd/output.txt"); fi != nil {
		t.Error("file should have been deleted", fi)
	}

	// Test keepalive.
	time.Sleep(2e9)

	statusReq := &WorkerStatusRequest{}
	statusRep := &WorkerStatusResponse{}
	tc.worker.Status(statusReq, statusRep)
	if len(statusRep.MirrorStatus) > 0 {
		t.Fatal("Processes still alive.")
	}
}

func TestEndToEndNegativeNotify(t *testing.T) {
	if os.Geteuid() == 0 {
		log.Println("This test should not run as root")
		return
	}

	tc := NewTestCase(t)
	defer tc.Clean()

	rep := tc.Run(WorkRequest{
		Binary: tc.FindBin("cat"),
		Argv:   []string{"cat", "output.txt"},
		Env:    testEnv(),
		Dir:    tc.tmp + "/wd",
	})

	if rep.Exit.ExitStatus() == 0 {
		t.Fatal("expect exit status != 0")
	}

	newContent := []byte("new content")
	hash := tc.master.cache.Save(newContent)
	updated := []*FileAttr{
		&FileAttr{
			Path:     tc.tmp + "/wd/output.txt",
			FileInfo: &os.FileInfo{Mode: fuse.S_IFREG | 0644, Size: int64(len(newContent))},
			Hash:     hash,
		},
	}
	tc.master.mirrors.queueFiles(nil, updated)

	rep = tc.Run(WorkRequest{
		Binary: tc.FindBin("cat"),
		Argv:   []string{"cat", "output.txt"},
		Env:    testEnv(),
		Dir:    tc.tmp + "/wd",
	})

	if rep.Exit.ExitStatus() != 0 {
		t.Fatal("expect exit status == 0", rep.Exit.ExitStatus())
	}
	log.Println("new content:", rep.Stdout)
	if string(rep.Stdout) != string(newContent) {
		t.Error("Mismatch", string(rep.Stdout), string(newContent))
	}
}

func TestEndToEndMove(t *testing.T) {
	if os.Geteuid() == 0 {
		log.Println("This test should not run as root")
		return
	}

	tc := NewTestCase(t)
	defer tc.Clean()

	rep := tc.Run(WorkRequest{
		Binary: tc.FindBin("mkdir"),
		Argv:   []string{"mkdir", "-p", "a/b/c"},
		Env:    testEnv(),
		Dir:    tc.tmp + "/wd",
	})
	if rep.Exit.ExitStatus() != 0 {
		t.Fatalf("mkdir should exit cleanly. Rep %v", rep)
	}
	rep = tc.Run(WorkRequest{
		Binary: tc.FindBin("mv"),
		Argv:   []string{"mv", "a", "q"},
		Env:    testEnv(),
		Dir:    tc.tmp + "/wd",
	})
	if rep.Exit.ExitStatus() != 0 {
		t.Fatalf("mv should exit cleanly. Rep %v", rep)
	}

	if fi, err := os.Lstat(tc.tmp + "/wd/q/b/c"); err != nil || !fi.IsDirectory() {
		t.Errorf("dir should have been moved. Err %v, fi %v", err, fi)
	}
}

func TestEndToEndStdout(t *testing.T) {
	if os.Geteuid() == 0 {
		log.Println("This test should not run as root")
		return
	}

	tc := NewTestCase(t)
	defer tc.Clean()

	err := os.Symlink("oldlink", tc.tmp+"/wd/symlink")
	if err != nil {
		t.Fatal("oldlink symlink", err)
	}

	shcmd := make([]byte, 1500)
	for i := 0; i < len(shcmd); i++ {
		shcmd[i] = 'a'
	}
	err = ioutil.WriteFile(tc.tmp+"/wd/file.txt", shcmd, 0644)
	if err != nil {
		t.Fatalf("WriteFile %#v", err)
	}

	rep := tc.Run(WorkRequest{
		Binary: tc.FindBin("cat"),
		Argv:   []string{"cat", "file.txt"},
		Env:    testEnv(),
		Dir:    tc.tmp + "/wd",
	})

	if string(rep.Stdout) != string(shcmd) {
		t.Errorf("Reply mismatch %s expect %s", string(rep.Stdout), string(shcmd))
	}
}

func TestEndToEndModeChange(t *testing.T) {
	if os.Geteuid() == 0 {
		log.Println("This test should not run as root")
		return
	}

	tc := NewTestCase(t)
	defer tc.Clean()

	err := ioutil.WriteFile(tc.tmp+"/wd/file.txt", []byte{42}, 0644)
	check(err)

	rep := tc.Run(WorkRequest{
		Binary: tc.FindBin("chmod"),
		Argv:   []string{"chmod", "a+x", "file.txt"},
		Env:    testEnv(),
		Dir:    tc.tmp + "/wd",
	})

	fi, err := os.Lstat(tc.tmp + "/wd/file.txt");
	check(err)

	if !fi.IsRegular() || fi.Mode & 0111 == 0 {
		t.Fatalf("wd/file.txt did not change mode: %o", fi.Mode)
	}
	if rep.Exit.ExitStatus() != 0 {
		t.Fatalf("chmod should exit cleanly. Rep %v", rep)
	}
}


func TestEndToEndSymlink(t *testing.T) {
	if os.Geteuid() == 0 {
		log.Println("This test should not run as root")
		return
	}

	tc := NewTestCase(t)
	defer tc.Clean()

	err := os.Symlink("oldlink", tc.tmp+"/wd/symlink")
	if err != nil {
		t.Fatal("oldlink symlink", err)
	}

	rep := tc.Run(WorkRequest{
		Binary: tc.FindBin("touch"),
		Argv:   []string{"touch", "file.txt"},
		Env:    testEnv(),
		Dir:    tc.tmp + "/wd",
	})

	if fi, err := os.Lstat(tc.tmp + "/wd/file.txt"); err != nil || !fi.IsRegular() || fi.Size != 0 {
		t.Fatalf("wd/file.txt was not created. Err: %v, fi: %v", err, fi)
	}
	if rep.Exit.ExitStatus() != 0 {
		t.Fatalf("touch should exit cleanly. Rep %v", rep)
	}
	rep = tc.Run(WorkRequest{
		Binary: tc.FindBin("ln"),
		Argv:   []string{"ln", "-sf", "foo", "symlink"},
		Env:    testEnv(),
		Dir:    tc.tmp + "/wd",
	})
	if rep.Exit.ExitStatus() != 0 {
		t.Fatalf("ln -s should exit cleanly. Rep %v", rep)
	}

	if fi, err := os.Lstat(tc.tmp + "/wd/symlink"); err != nil || !fi.IsSymlink() {
		t.Errorf("should have symlink. Err %v, fi %v", err, fi)
	}
}

func TestEndToEndShutdown(t *testing.T) {
	if os.Geteuid() == 0 {
		log.Println("This test should not run as root")
		return
	}

	tc := NewTestCase(t)
	defer tc.Clean()

	// In the test, shutdown doesn't really exit the worker, since
	// we can't stop the already running accept(); retry would
	// cause the test to hang.
	tc.master.retryCount = 0

	req := WorkRequest{
		Binary: tc.FindBin("touch"),
		Argv:   []string{"touch", "file.txt"},
		Env:    testEnv(),
		Dir:    tc.tmp + "/wd",
	}
	rep := tc.Run(req)

	hostname, err := os.Hostname()
	if err != nil {
		t.Fatalf("hostname error %v", err)
	}
	conn, err := DialTypedConnection(
		fmt.Sprintf("%s:%d", hostname, tc.workerPort), RPC_CHANNEL, tc.secret)
	if conn == nil {
		t.Fatal("DialTypedConnection to shutdown worker: ", err)
	}

	stopReq := 1
	stopRep := 1
	err = rpc.NewClient(conn).Call("WorkerDaemon.Shutdown", &stopReq, &stopRep)
	if err != nil {
		t.Errorf("Shutdown insuccessful: %v", err)
	}

	rpcConn := OpenSocketConnection(tc.socket, RPC_CHANNEL)
	err = rpc.NewClient(rpcConn).Call("LocalMaster.Run", &req, &rep)
	if err == nil {
		t.Error("LocalMaster.Run should fail after shutdown")
	}

	// TODO - check that DialTypedConnection to worker stops working?
}

func TestEndToEndSpecialEntries(t *testing.T) {
	tc := NewTestCase(t)
	defer tc.Clean()

	readlink, _ := filepath.EvalSymlinks(tc.FindBin("readlink"))
	req := WorkRequest{
		Binary: readlink,
		Argv:   []string{"readlink", "proc/self/exe"},
		Env:    testEnv(),
		Dir:    "/",
	}
	rep := tc.Run(req)

	if rep.Exit.ExitStatus() != 0 {
		t.Fatalf("readlink should exit cleanly. Rep %v", rep)
	}

	out, _ := filepath.EvalSymlinks(strings.TrimRight(rep.Stdout, "\n"))
	if out != readlink {
		t.Errorf("proc/self/exe point to wrong location: got %q, expect %q", out, readlink)
	}

	req = WorkRequest{
		Binary: tc.FindBin("ls"),
		Argv:   []string{"ls", "proc/misc"},
		Env:    testEnv(),
		Dir:    "/",
	}
	rep = tc.Run(req)
	if rep.Exit.ExitStatus() == 0 {
		t.Fatalf("ls should have failed", rep)
	}
}

func TestEndToEndEnvironment(t *testing.T) {
	tc := NewTestCase(t)
	defer tc.Clean()

	req := WorkRequest{
		Binary: tc.FindBin("sh"),
		Argv:   []string{"sh", "-c", "echo $MAGIC"},
		Env:    testEnv(),
		Dir:    "/",
	}
	req.Env = append(req.Env, "MAGIC=777")
	rep := tc.Run(req)
	out := strings.TrimRight(rep.Stdout, "\n")
	if out != "777" {
		t.Errorf("environment got lost. Got %q", out)
	}
}
