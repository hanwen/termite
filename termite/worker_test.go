package termite

import (
	"fmt"
	"github.com/hanwen/go-fuse/fuse"
	"io/ioutil"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

type testCase struct {
	workers    []*WorkerDaemon
	workerOpts *WorkerOptions

	master          *Master
	coordinator     *Coordinator
	secret          []byte
	tmp             string
	wd              string
	socket          string
	coordinatorPort int
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

func (me *testCase) StartWorker(coordinator string) {
	worker := NewWorkerDaemon(me.workerOpts)
	// TODO -racy. 
	me.workers = append(me.workers, worker)
	worker.RunWorkerServer(0, coordinator)
}

func NewTestCase(t *testing.T) *testCase {
	if os.Geteuid() == 0 {
		t.Fatal("This test should not run as root")
	}

	me := new(testCase)
	me.tester = t
	me.secret = RandomBytes(20)
	me.tmp, _ = ioutil.TempDir("", "")

	me.startFdCount = me.fdCount()
	workerTmp := me.tmp + "/worker-tmp"
	os.Mkdir(workerTmp, 0700)
	me.workerOpts = &WorkerOptions{
		Secret:         me.secret,
		TempDir:        workerTmp,
		CacheDir:       me.tmp + "/worker-cache",
		Jobs:           1,
		ReportInterval: 0.1,
	}

	me.wd = me.tmp + "/wd"
	os.MkdirAll(me.wd, 0755)

	me.coordinator = NewCoordinator(me.secret)
	go me.coordinator.PeriodicCheck()
	go me.coordinator.ServeHTTP(0)
	for me.coordinator.listener == nil {
		time.Sleep(1e6)
	}
	coordinatorAddr := me.coordinator.listener.Addr()
	_, portString, _ := net.SplitHostPort(coordinatorAddr.String())
	fmt.Sscanf(portString, "%d", &me.coordinatorPort)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		masterCache := NewContentCache(me.tmp + "/master-cache")
		masterOpts := MasterOptions{
			WritableRoot: me.wd,
			RetryCount:   3,
			Secret:       me.secret,
			MaxJobs:      1,
			Coordinator:  coordinatorAddr.String(),
		}
		me.master = NewMaster(masterCache, &masterOpts)
		me.master.SetKeepAlive(0.5, 0.5)
		me.socket = me.wd + "/master-socket"
		go me.master.Start(me.socket)
		wg.Done()
	}()
	go me.StartWorker(coordinatorAddr.String())
	wg.Wait()

	for i := 0; me.coordinator.WorkerCount() == 0 && i < 10; i++ {
		time.Sleep(50e6)
	}

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
	me.master.mirrors.dropConnections()
	for _, w := range me.workers {
		req := ShutdownRequest{}
		rep := ShutdownResponse{}
		err := w.Shutdown(&req, &rep)
		if err != nil {
			me.tester.Fatal("Worker shutdown error:", err)
		}
	}

	// TODO - should have explicit worker shutdown routine.
	me.coordinator.Shutdown()
	time.Sleep(0.1e9)
	os.RemoveAll(me.tmp)

	// TODO - there are still some persistent leaks here.
	if false && me.fdCount() > me.startFdCount {
		me.tester.Errorf("Fd leak. Start: %d, end %d", me.startFdCount, me.fdCount())
		dir := "/proc/self/fd"
		entries, _ := ioutil.ReadDir(dir)
		for _, e := range entries {
			l, _ := os.Readlink(filepath.Join(dir, e.Name))
			me.tester.Logf("%s -> %q", e.Name, l)
		}
	}
}

func (me *testCase) refresh() {
	me.master.refreshAttributeCache()
}

func (me *testCase) RunFail(req WorkRequest) (rep WorkResponse) {
	rep = me.Run(req, true)
	if rep.Exit.ExitStatus() == 0 {
		me.tester.Fatalf("expect exit status != 0 for %v", req)
	}
	return rep
}

func (me *testCase) RunSuccess(req WorkRequest) (rep WorkResponse) {
	rep = me.Run(req, true)
	if rep.Exit.ExitStatus() != 0 {
		me.tester.Fatalf("Got exit status %d for %v", rep.Exit.ExitStatus(), req)
	}
	return rep
}

func (me *testCase) Run(req WorkRequest, mustExit bool) (rep WorkResponse) {
	rpcConn := OpenSocketConnection(me.socket, RPC_CHANNEL, 1e7)
	client := rpc.NewClient(rpcConn)
	if req.Env == nil {
		req.Env = testEnv()
	}
	if req.Dir == "" {
		req.Dir = me.wd
	}
	if req.Binary == "" {
		req.Binary = me.FindBin(req.Argv[0])
	}
	err := client.Call("LocalMaster.Run", &req, &rep)
	if mustExit && err != nil {
		me.tester.Fatal("LocalMaster.Run: ", err)
	}
	client.Close()
	return rep
}

// Simple end-to-end test.  It skips the chroot, but should give a
// basic assurance that things work as expected.
func TestEndToEndBasic(t *testing.T) {
	tc := NewTestCase(t)
	defer tc.Clean()

	req := WorkRequest{
		StdinId: ConnectionId(),
		Argv:    []string{"tee", "output.txt"},
	}

	// TODO - should separate dial/listen in the daemons?
	stdinConn := OpenSocketConnection(tc.socket, req.StdinId, 10e6)
	go func() {
		stdinConn.Write([]byte("hello"))
		stdinConn.Close()
	}()

	tc.RunSuccess(req)
	content, err := ioutil.ReadFile(tc.wd + "/output.txt")
	if err != nil {
		t.Error(err)
	}
	if string(content) != "hello" {
		t.Error("content:", content)
	}

	tc.RunSuccess(WorkRequest{
		Argv: []string{"rm", "output.txt"},
	})

	if fi, _ := os.Lstat(tc.wd + "/output.txt"); fi != nil {
		t.Error("file should have been deleted", fi)
	}

	// Test keepalive.
	time.Sleep(2e9)
	statusReq := &WorkerStatusRequest{}
	statusRep := &WorkerStatusResponse{}
	for _, w := range tc.workers {
		w.Status(statusReq, statusRep)
		if len(statusRep.MirrorStatus) > 0 {
			t.Fatal("Processes still alive.")
		}
	}
}

func TestEndToEndFullPath(t *testing.T) {
	tc := NewTestCase(t)
	defer tc.Clean()

	rpcConn := OpenSocketConnection(tc.socket, RPC_CHANNEL, 1e7)
	client := rpc.NewClient(rpcConn)
	req := WorkRequest{
		Binary: "true",
		Argv:   []string{"true"},
		Env:    testEnv(),
		Dir:    tc.wd,
	}
	rep := &WorkResponse{}
	err := client.Call("LocalMaster.Run", &req, &rep)
	msg := "nil"
	if err != nil {
		msg = err.Error()
	}
	t.Log("Call error:", msg)
	if !strings.Contains(msg, "absolute") {
		t.Fatalf("master should demand absolute path: %v", msg)
	}
	client.Close()
}

func TestEndToEndFormatError(t *testing.T) {
	tc := NewTestCase(t)
	defer tc.Clean()

	ioutil.WriteFile(tc.wd+"/ls.sh", []byte("ls"), 0755)

	rpcConn := OpenSocketConnection(tc.socket, RPC_CHANNEL, 1e7)
	client := rpc.NewClient(rpcConn)
	req := WorkRequest{
		Binary: tc.wd + "/ls.sh",
		Argv:   []string{"ls.sh"},
		Env:    testEnv(),
		Dir:    tc.wd,
	}
	rep := &WorkResponse{}
	err := client.Call("LocalMaster.Run", &req, &rep)
	t.Log(err)
	client.Close()
}

func TestEndToEndExec(t *testing.T) {
	tc := NewTestCase(t)
	defer tc.Clean()

	tc.RunSuccess(WorkRequest{
		Argv: []string{"true"},
	})
}

func TestEndToEndNegativeNotify(t *testing.T) {
	tc := NewTestCase(t)
	defer tc.Clean()

	rep := tc.RunFail(WorkRequest{
		Argv: []string{"cat", "output.txt"},
	})

	newContent := []byte("new content")
	hash := tc.master.cache.Save(newContent)
	updated := []*FileAttr{
		&FileAttr{
			Path: tc.wd[1:] + "/output.txt",
			FileInfo: &os.FileInfo{
				Mode: fuse.S_IFREG | 0644,
				Size: int64(len(newContent)),
			},
			Hash: hash,
		},
	}
	fset := FileSet{Files: updated}
	tc.master.replay(fset)

	rep = tc.RunSuccess(WorkRequest{
		Argv: []string{"cat", "output.txt"},
	})
	if string(rep.Stdout) != string(newContent) {
		t.Error("Mismatch", string(rep.Stdout), string(newContent))
	}
}

func TestEndToEndMoveFile(t *testing.T) {
	tc := NewTestCase(t)
	defer tc.Clean()

	err := ioutil.WriteFile(tc.wd+"/e2e-move.txt", []byte{42}, 0644)
	check(err)

	tc.refresh()

	tc.RunSuccess(WorkRequest{
		Argv: []string{"mv", "e2e-move.txt", "e2e-new.txt"},
	})

	c, err := ioutil.ReadFile(tc.wd + "/e2e-new.txt")
	check(err)
	if len(c) != 1 {
		t.Fatalf("Moved file missing content: %s", c)
	}
}

func TestEndToEndMove(t *testing.T) {
	tc := NewTestCase(t)
	defer tc.Clean()

	tc.RunSuccess(WorkRequest{
		Argv: []string{"mkdir", "-p", "a/b/c"},
	})
	tc.RunSuccess(WorkRequest{
		Argv: []string{"mv", "a", "q"},
	})

	if fi, err := os.Lstat(tc.wd + "/q/b/c"); err != nil || !fi.IsDirectory() {
		t.Errorf("dir should have been moved. Err %v, fi %v", err, fi)
	}
}

func TestEndToEndStdout(t *testing.T) {
	tc := NewTestCase(t)
	defer tc.Clean()

	err := os.Symlink("oldlink", tc.wd+"/symlink")
	check(err)

	shcmd := make([]byte, 1500)
	for i := 0; i < len(shcmd); i++ {
		shcmd[i] = 'a'
	}
	err = ioutil.WriteFile(tc.tmp+"/wd/file.txt", shcmd, 0644)
	check(err)
	tc.refresh()

	rep := tc.RunSuccess(WorkRequest{
		Argv: []string{"cat", "file.txt"},
	})

	if string(rep.Stdout) != string(shcmd) {
		t.Errorf("Reply mismatch %s expect %s", string(rep.Stdout), string(shcmd))
	}
}

func TestEndToEndModeChange(t *testing.T) {
	tc := NewTestCase(t)
	defer tc.Clean()

	err := ioutil.WriteFile(tc.tmp+"/wd/file.txt", []byte{42}, 0644)
	check(err)
	tc.refresh()

	tc.RunSuccess(WorkRequest{
		Argv: []string{"chmod", "a+x", "file.txt"},
	})

	fi, err := os.Lstat(tc.wd + "/file.txt")
	check(err)

	if !fi.IsRegular() || fi.Mode&0111 == 0 {
		t.Fatalf("wd/file.txt did not change mode: %o", fi.Mode)
	}
}

func TestEndToEndSymlink(t *testing.T) {
	tc := NewTestCase(t)
	defer tc.Clean()

	err := os.Symlink("oldlink", tc.tmp+"/wd/symlink")
	if err != nil {
		t.Fatal("oldlink symlink", err)
	}

	tc.RunSuccess(WorkRequest{
		Argv: []string{"touch", "file.txt"},
	})

	if fi, err := os.Lstat(tc.wd + "/file.txt"); err != nil || !fi.IsRegular() || fi.Size != 0 {
		t.Fatalf("wd/file.txt was not created. Err: %v, fi: %v", err, fi)
	}
	tc.RunSuccess(WorkRequest{
		Argv: []string{"ln", "-sf", "foo", "symlink"},
	})

	if fi, err := os.Lstat(tc.wd + "/symlink"); err != nil || !fi.IsSymlink() {
		t.Errorf("should have symlink. Err %v, fi %v", err, fi)
	}
}

func TestEndToEndShutdown(t *testing.T) {
	tc := NewTestCase(t)
	defer tc.Clean()

	// In the test, shutdown doesn't really exit the worker, since
	// we can't stop the already running accept(); retry would
	// cause the test to hang.
	tc.master.options.RetryCount = 0

	req := WorkRequest{
		Argv: []string{"touch", "file.txt"},
	}
	tc.RunSuccess(req)

	addresses := []string{}
	for addr := range tc.coordinator.workers {
		addresses = append(addresses, addr)
	}
	for _, a := range addresses {
		cl := http.Client{}
		_, err := cl.Get(fmt.Sprintf("http://localhost:%d/killworker?host=%s", tc.coordinatorPort, a))
		check(err)
	}
}

func TestEndToEndLogFile(t *testing.T) {
	tc := NewTestCase(t)
	defer tc.Clean()
	fn := tc.wd + "/logfile.txt"
	ioutil.WriteFile(fn, []byte("magic string"), 0644)
	for _, w := range tc.workers {
		w.LogFileName = fn
	}
	addresses := []string{}
	for addr := range tc.coordinator.workers {
		addresses = append(addresses, addr)
	}
	for _, a := range addresses {
		cl := http.Client{}
		req, err := cl.Get(fmt.Sprintf("http://localhost:%d/log?host=%s", tc.coordinatorPort, a))
		check(err)

		data, _ := ioutil.ReadAll(req.Body)
		if !strings.Contains(string(data), "magic string") {
			t.Errorf("'magic string' missing. Got: %q", data)
		}
	}
}

func TestEndToEndSpecialEntries(t *testing.T) {
	tc := NewTestCase(t)
	defer tc.Clean()

	readlink, _ := filepath.EvalSymlinks(tc.FindBin("readlink"))
	req := WorkRequest{
		Argv: []string{"readlink", "proc/self/exe"},
		Dir:  "/",
	}
	rep := tc.RunSuccess(req)

	out, _ := filepath.EvalSymlinks(strings.TrimRight(rep.Stdout, "\n"))
	if out != readlink {
		t.Errorf("proc/self/exe point to wrong location: got %q, expect %q", out, readlink)
	}
}

func TestEndToEndProcDeny(t *testing.T) {
	tc := NewTestCase(t)
	defer tc.Clean()

	req := WorkRequest{
		Argv: []string{"ls", "proc/misc"},
		Dir:  "/",
	}
	tc.RunFail(req)
}

func TestEndToEndEnvironment(t *testing.T) {
	tc := NewTestCase(t)
	defer tc.Clean()

	req := WorkRequest{
		Argv: []string{"sh", "-c", "echo $MAGIC"},
		Dir:  "/",
	}
	req.Env = append(req.Env, "MAGIC=777")
	rep := tc.RunSuccess(req)
	out := strings.TrimRight(rep.Stdout, "\n")
	if out != "777" {
		t.Errorf("environment got lost. Got %q", out)
	}
}

func TestEndToEndLinkReap(t *testing.T) {
	tc := NewTestCase(t)
	defer tc.Clean()

	// TODO - drop this.
	ioutil.WriteFile(tc.wd+"/file.txt", []byte{42}, 0644)
	tc.refresh()

	req := WorkRequest{
		Argv: []string{"sh", "-c", "echo hello > file.txt ; ln file.txt foo.txt"},
	}
	tc.RunSuccess(req)
	if fi, err := os.Lstat(tc.wd + "/foo.txt"); err != nil || !fi.IsRegular() || fi.Size != 6 {
		t.Fatalf("wd/foo.txt was not created. Err: %v, fi: %v", err, fi)
	}
}

func TestEndToEndKillChild(t *testing.T) {
	tc := NewTestCase(t)
	defer tc.Clean()

	req := WorkRequest{
		Argv: []string{"sh", "-c", "sleep 1s ; touch file.txt"},
	}
	complete := make(chan int)
	go func() {
		tc.Run(req, false)
		complete <- 1
	}()

	time.Sleep(0.5e9)
	// force shutdown.
	tc.master.mirrors.dropConnections()
	time.Sleep(0.5e9)
	<-complete
}
