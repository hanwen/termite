package termite

import (
	"fmt"
	"io/ioutil"
	"log"
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

	"github.com/hanwen/go-fuse/fuse"
	"github.com/hanwen/go-fuse/splice"
	"github.com/hanwen/termite/attr"
	"github.com/hanwen/termite/cba"
)

var _ = log.Println

type testCase struct {
	workers    []*Worker
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

func (tc *testCase) FindBin(name string) string {
	full, err := exec.LookPath(name)
	if err != nil {
		tc.tester.Fatal("looking for binary:", err)
	}

	full, err = filepath.EvalSymlinks(full)
	if err != nil {
		tc.tester.Fatal("EvalSymlinks:", err)
	}
	return full
}

func testEnv() []string {
	return []string{
		"PATH=/bin:/usr/bin",
		"USER=nobody",
	}
}

func (tc *testCase) StartWorker() {
	worker := NewWorker(tc.workerOpts)
	tc.workers = append(tc.workers, worker)
	go worker.RunWorkerServer()
}

func pickPort(t *testing.T) int {
	l, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatalf("listen %v", err)
	}
	p := l.Addr().(*net.TCPAddr).Port
	l.Close()
	return p
}

func NewTestCase(t *testing.T) *testCase {
	tc := new(testCase)
	tc.tester = t
	tc.secret = RandomBytes(20)
	tc.tmp, _ = ioutil.TempDir("", "")

	tc.startFdCount = tc.fdCount()
	workerTmp := tc.tmp + "/worker-tmp"
	os.Mkdir(workerTmp, 0700)

	cOpts := CoordinatorOptions{
		Secret: tc.secret,
	}
	tc.coordinator = NewCoordinator(&cOpts)
	go tc.coordinator.PeriodicCheck()

	tc.coordinatorPort = pickPort(t)
	go tc.coordinator.ServeHTTP(tc.coordinatorPort)
	coordinatorAddr := fmt.Sprintf("localhost:%d", tc.coordinatorPort)
	mkbox, err := filepath.Abs("../bin/mkbox/mkbox")
	if err != nil {
		t.Fatalf("filepath.Abs(\"mkbox\"): %v", err)
	}

	tc.workerOpts = &WorkerOptions{
		Secret:  tc.secret,
		TempDir: workerTmp,
		StoreOptions: cba.StoreOptions{
			Dir: tc.tmp + "/worker-cache",
		},
		Jobs:           1,
		ReportInterval: 100 * time.Millisecond,
		Coordinator:    coordinatorAddr,
		PortRetry:      10,
		Mkbox:          mkbox,
	}

	tc.wd = tc.tmp + "/wd"
	os.MkdirAll(tc.wd, 0755)

	tc.socket = tc.wd + "/master-socket"
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		masterOpts := MasterOptions{
			WritableRoot:  tc.wd,
			RetryCount:    2,
			Secret:        tc.secret,
			MaxJobs:       1,
			Coordinator:   coordinatorAddr,
			KeepAlive:     500 * time.Millisecond,
			Period:        500 * time.Millisecond,
			ExposePrivate: true,
			StoreOptions: cba.StoreOptions{
				Dir: tc.tmp + "/master-cache",
			},
			Socket: tc.socket,
		}
		tc.master = NewMaster(&masterOpts)
		go tc.master.Start()
		for i := 0; i < 10; i++ {
			if fi, _ := os.Lstat(tc.socket); fi != nil {
				break
			}
			time.Sleep(10e6)
		}
		wg.Done()
	}()
	tc.StartWorker()
	wg.Wait()
	for i := 0; tc.coordinator.WorkerCount() == 0; i++ {
		if i > 10 {
			t.Fatal("no live workers after 10 tries")
		}
		time.Sleep(50e6)
	}

	return tc
}

func (tc *testCase) fdCount() int {
	entries, err := ioutil.ReadDir("/proc/self/fd")
	if err != nil {
		tc.tester.Fatal("ReadDir fd", err)
	}
	return len(entries)
}

func (tc *testCase) Clean() {
	tc.master.mirrors.dropConnections()
	tc.master.quit <- 1

	tc.coordinator.killAll(false)
	splice.ClearSplicePool()

	// TODO - should have explicit worker shutdown routine.
	tc.coordinator.Shutdown()

	// TODO - should sleep until everything has exited.
	time.Sleep(500 * time.Millisecond)
	os.RemoveAll(tc.tmp)

	// TODO - there are still some persistent leaks here.
	if false && tc.fdCount() > tc.startFdCount {
		tc.tester.Errorf("Fd leak. Start: %d, end %d", tc.startFdCount, tc.fdCount())
		dir := "/proc/self/fd"
		entries, _ := ioutil.ReadDir(dir)
		for _, e := range entries {
			l, _ := os.Readlink(filepath.Join(dir, e.Name()))
			tc.tester.Logf("%s -> %q", e.Name(), l)
		}
	}
}

func (tc *testCase) refresh() {
	tc.master.refreshAttributeCache()
}

func (tc *testCase) RunFail(req WorkRequest) (rep WorkResponse) {
	rep = tc.Run(req, true)
	if rep.Exit.ExitStatus() == 0 {
		tc.tester.Fatalf("expect exit status != 0 for %v", req)
	}
	return rep
}

func (tc *testCase) RunSuccess(req WorkRequest) (rep WorkResponse) {
	rep = tc.Run(req, true)
	if rep.Exit.ExitStatus() != 0 {
		tc.tester.Fatalf("Got exit status %d for %v. stderr:\n%s", rep.Exit.ExitStatus(), req, rep.Stderr)
	}
	return rep
}

func (tc *testCase) Run(req WorkRequest, mustExit bool) (rep WorkResponse) {
	rpcConn := OpenSocketConnection(tc.socket, RPC_CHANNEL, 1e7)
	client := rpc.NewClient(rpcConn)
	if req.Env == nil {
		req.Env = testEnv()
	}
	if req.Dir == "" {
		req.Dir = tc.wd
	}
	if req.Binary == "" {
		req.Binary = tc.FindBin(req.Argv[0])
	}
	err := client.Call("LocalMaster.Run", &req, &rep)
	if mustExit && err != nil {
		tc.tester.Fatal("LocalMaster.Run: ", err)
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
}

func TestEndToEndKeepalive(t *testing.T) {
	tc := NewTestCase(t)
	defer tc.Clean()

	req := WorkRequest{
		Argv: []string{"touch", "output.txt"},
	}

	tc.RunSuccess(req)
	if _, err := os.Lstat(tc.wd + "/output.txt"); err != nil {
		t.Fatalf("output file not found")
	}

	// Test keepalive.
	time.Sleep(2 * time.Second)
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
	hash := tc.master.contentStore.Save(newContent)
	updated := []*attr.FileAttr{
		{
			Path: tc.wd[1:] + "/output.txt",
			Attr: &fuse.Attr{
				Mode: fuse.S_IFREG | 0644,
				Size: uint64(len(newContent)),
			},
			Hash: hash,
		},
	}
	fset := attr.FileSet{Files: updated}
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

	if fi, err := os.Lstat(tc.wd + "/q/b/c"); err != nil || !fi.IsDir() {
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

	if fi.Mode()&os.ModeType != 0 || fi.Mode().Perm()&0111 == 0 {
		t.Fatalf("wd/file.txt did not change mode: %o", fi.Mode().Perm())
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

	if fi, err := os.Lstat(tc.wd + "/file.txt"); err != nil || fi.Mode()&os.ModeType != 0 || fi.Size() != 0 {
		t.Fatalf("wd/file.txt was not created. Err: %v, fi: %v", err, fi)
	}
	tc.RunSuccess(WorkRequest{
		Argv: []string{"ln", "-sf", "foo", "symlink"},
	})

	if fi, err := os.Lstat(tc.wd + "/symlink"); err != nil || fi.Mode()&os.ModeSymlink == 0 {
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
		w.options.LogFileName = fn
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
	if fi, err := os.Lstat(tc.wd + "/foo.txt"); err != nil || fi.Mode()&os.ModeType != 0 || fi.Size() != 6 {
		t.Fatalf("wd/foo.txt was not created. Err: %v, fi: %v", err, fi)
	}
}

// TODO - every once in a while this fails to unmount, with disastrous
// results.
func DisabledTestEndToEndKillChild(t *testing.T) {
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
	time.Sleep(0.6e9)
	<-complete
}

func TestEndToEndDenyPrivate(t *testing.T) {
	tc := NewTestCase(t)
	defer tc.Clean()

	p := tc.wd
	for p != "" {
		os.Chmod(p, 0755)
		p, _ = SplitPath(p)
	}

	err := ioutil.WriteFile(tc.wd+"/file.txt", []byte{42}, 0644)
	check(err)
	err = ioutil.WriteFile(tc.wd+"/forbidden.txt", []byte{42}, 0600)
	check(err)

	tc.master.options.ExposePrivate = false
	req := WorkRequest{
		Argv: []string{"cat", "file.txt"},
	}
	tc.RunSuccess(req)
	req = WorkRequest{
		Argv: []string{"cat", "forbidden.txt"},
	}
	tc.RunFail(req)
}
