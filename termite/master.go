package termite

import (
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/rpc"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/hanwen/go-fuse/fuse"
	"github.com/hanwen/go-fuse/splice"
	"github.com/hanwen/termite/attr"
	"github.com/hanwen/termite/cba"
	"github.com/hanwen/termite/stats"
)

type Master struct {
	contentStore  *cba.Store
	fileServer    *attr.Server
	fileServerRpc *rpc.Server
	timing        *stats.TimerStats
	excluded      map[string]bool
	attributes    *attr.AttributeCache
	mirrors       *mirrorConnections
	taskIds       chan int
	options       *MasterOptions
	replayChannel chan *replayRequest
	quit          chan int
	dialer        connDialer
}

// Immutable state and options for master.
type MasterOptions struct {
	cba.StoreOptions

	WritableRoot string
	SourceRoot   string

	// How often a failed should be retried on another worker.
	RetryCount int

	// List of files that should not be served
	Excludes []string

	// If set, also serve files that have no group/other permissions
	ExposePrivate bool

	// Address of the coordinator.
	Coordinator string

	Secret []byte

	MaxJobs int

	// Turns on internal consistency checks. Expensive.
	Paranoia bool

	// On startup, fault-in all files.
	FetchAll bool

	// How often to do periodic householding work.
	Period time.Duration

	// How long to keep mirrors alive.
	KeepAlive time.Duration

	// Cache hashes in filesystem extended attributes.
	XAttrCache bool

	Uid int

	// The log file.  This is to ensure we don't export or hash
	// the log file.
	LogFile string

	// Path to the socket file.
	Socket string
}

type replayRequest struct {
	// Hash => Tempfile name.
	NewFiles map[string][]string

	// Path => Hash
	DelFileHashes map[string]string
	Files         []*attr.FileAttr
	Done          chan int
}

func (m *Master) uncachedGetAttr(name string) (rep *attr.FileAttr) {
	rep = &attr.FileAttr{Path: name}
	p := m.path(name)

	if p == m.options.Socket || p == m.options.LogFile {
		return rep
	}

	fi, _ := os.Lstat(p)

	// We don't want to expose the master's private files to the
	// world.
	if !m.options.ExposePrivate && fi != nil && fi.Mode().Perm()&0077 == 0 {
		log.Printf("Denied access to private file %q", name)
		return rep
	}

	if m.excluded[name] {
		log.Printf("Denied access to excluded file %q", name)
		return rep
	}
	rep.Attr = fuse.ToAttr(fi)

	xattrPossible := rep.IsRegular() && m.options.XAttrCache && rep.Uid == uint32(m.options.Uid) && ((m.options.WritableRoot != "" && strings.HasPrefix(p, m.options.WritableRoot)) ||
		(m.options.SourceRoot != "" && strings.HasPrefix(p, m.options.SourceRoot)))

	if xattrPossible {
		cur := attr.EncodedAttr{}
		cur.FromAttr(rep.Attr)

		disk := attr.EncodedAttr{}
		diskHash := disk.ReadXAttr(p)

		if diskHash != nil && cur.Eq(&disk) && m.contentStore.Has(string(diskHash)) {
			rep.Hash = string(diskHash)
			return rep
		}
	}

	if fi != nil {
		m.fillContent(rep)
		if xattrPossible {
			if rep.Mode&0222 == 0 {
				os.Chmod(p, os.FileMode(rep.Mode|0200))
			}
			rep.WriteXAttr(p)
			if rep.Mode&0222 == 0 {
				os.Chmod(p, os.FileMode(rep.Mode))
			}
		}
	}
	return rep
}

func (m *Master) fillContent(rep *attr.FileAttr) {
	if rep.IsSymlink() || rep.IsDir() {
		rep.ReadFromFs(m.path(rep.Path), m.options.Hash)
	} else if rep.IsRegular() {
		fullPath := m.path(rep.Path)
		rep.Hash = m.contentStore.SavePath(fullPath)
		if rep.Hash == "" {
			// Typically happens if we want to open /etc/shadow as normal user.
			log.Println("fillContent returning EPERM for", rep.Path)
			rep.Attr = nil
		}
	}
}

func (m *Master) path(n string) string {
	return "/" + n
}

func NewMaster(options *MasterOptions) *Master {
	m := &Master{
		taskIds:       make(chan int, 100),
		replayChannel: make(chan *replayRequest, 1),
		quit:          make(chan int, 0),
		timing:        stats.NewTimerStats(),
	}
	m.contentStore = cba.NewStore(&options.StoreOptions, m.timing)

	o := *options
	if o.Period <= 0.0 {
		o.Period = 60.0
	}
	o.Uid = os.Getuid()
	if o.SourceRoot != "" {
		o.SourceRoot, _ = filepath.Abs(o.SourceRoot)
		o.SourceRoot, _ = filepath.EvalSymlinks(o.SourceRoot)
	}
	if o.Socket != "" {
		o.Socket, _ = filepath.Abs(o.Socket)
	}
	if o.LogFile != "" {
		o.LogFile, _ = filepath.Abs(o.LogFile)
	}

	m.options = &o
	m.dialer = newWorkerDialer(o.Secret)
	m.excluded = make(map[string]bool)
	for _, e := range options.Excludes {
		m.excluded[e] = true
	}

	m.mirrors = newMirrorConnections(
		m, options.Coordinator, options.MaxJobs)
	m.mirrors.keepAlive = options.KeepAlive
	m.attributes = attr.NewAttributeCache(func(n string) *attr.FileAttr {
		return m.uncachedGetAttr(n)
	},
		func(n string) *fuse.Attr {
			fi, _ := os.Lstat(m.path(n))
			return fuse.ToAttr(fi)
		})
	m.fileServer = attr.NewServer(m.attributes, m.timing)

	m.CheckPrivate()

	// Generate taskids.
	go func() {
		i := 0
		for {
			m.taskIds <- i
			i++
		}
	}()

	// Make sure we update the filesystem and attributes together.
	go func() {
		for {
			r := <-m.replayChannel
			m.replayFileModifications(r.Files, r.DelFileHashes, r.NewFiles)
			r.Done <- 1
		}
	}()

	return m
}

func (m *Master) CheckPrivate() {
	if m.options.ExposePrivate {
		return
	}
	d := m.options.WritableRoot
	for d != "" {
		fi, err := os.Lstat(d)
		if err != nil {
			log.Fatal("CheckPrivate:", err)
		}
		if fi != nil && fi.Mode().Perm()&0077 == 0 {
			log.Fatalf("Error: dir %q is mode %o.", d, fi.Mode().Perm())
		}
		d, _ = SplitPath(d)
	}
}

// Fetch in the background.
func (m *Master) FetchAll() {
	wg := sync.WaitGroup{}
	last := ""
	for _, r := range []string{m.options.WritableRoot, m.options.SourceRoot} {
		if last == r || r == "" {
			continue
		}
		last = r
		wg.Add(1)

		go func(p string) {
			log.Println("Prefetch", p)
			m.fetchAll(strings.TrimLeft(p, "/"))
			wg.Done()
		}(r)
	}
	wg.Wait()
	log.Println("FetchAll done")
}

func (m *Master) Start() {
	if m.options.FetchAll {
		go m.FetchAll()
	}
	go localStart(m, m.options.Socket)
	m.waitForExit()
}

func (m *Master) createMirror(addr string, jobs int) (*mirrorConnection, error) {
	closeMe := []io.ReadWriteCloser{}
	defer func() {
		for _, c := range closeMe {
			c.Close()
		}
	}()
	mux, err := m.dialer.Dial(addr)
	if err != nil {
		return nil, err
	}

	conn, err := mux.Open(RPC_CHANNEL)
	if err != nil {
		return nil, err
	}

	defer conn.Close()

	rpcId := ConnectionId()
	rpcConn, err := mux.Open(rpcId)
	if err != nil {
		return nil, err
	}
	closeMe = append(closeMe, rpcConn)

	revId := ConnectionId()
	revConn, err := mux.Open(revId)
	if err != nil {
		return nil, err
	}
	closeMe = append(closeMe, revConn)

	contentId := ConnectionId()
	contentConn, err := mux.Open(contentId)
	if err != nil {
		return nil, err
	}
	closeMe = append(closeMe, contentConn)

	revContentId := ConnectionId()
	revContentConn, err := mux.Open(revContentId)
	if err != nil {
		return nil, err
	}
	closeMe = append(closeMe, revContentConn)

	req := CreateMirrorRequest{
		RpcId:        rpcId,
		RevRpcId:     revId,
		ContentId:    contentId,
		RevContentId: revContentId,
		WritableRoot: m.options.WritableRoot,
		MaxJobCount:  jobs,
	}
	rep := CreateMirrorResponse{}
	cl := rpc.NewClient(conn)
	err = cl.Call("Worker.CreateMirror", &req, &rep)
	cl.Close()

	if err != nil {
		return nil, err
	}
	closeMe = nil

	log.Print("serving fileServerRpc on ", revConn.(net.Conn).LocalAddr())
	go attr.ServeRPC(m.fileServer, revConn)

	go m.contentStore.ServeConn(revContentConn)

	mc := &mirrorConnection{
		master:             m,
		rpcClient:          rpc.NewClient(rpcConn),
		contentClient:      m.contentStore.NewClient(contentConn),
		reverseConnection:  revConn,
		reverseContentConn: revContentConn,
		maxJobs:            rep.GrantedJobCount,
		availableJobs:      rep.GrantedJobCount,
	}
	mc.fileSetWaiter = attr.NewFileSetWaiter(func(fset attr.FileSet) error {
		return mc.replay(fset)
	})

	return mc, nil
}

func (m *Master) runOnMirror(mirror *mirrorConnection, req *WorkRequest, rep *WorkResponse) error {
	m.mirrors.stats.Enter("send")
	err := m.attributes.Send(mirror)
	m.mirrors.stats.Exit("send")
	if err != nil {
		return err
	}

	defer m.mirrors.jobDone(mirror)

	// Tunnel stdin.
	if req.StdinConn != nil {
		mux, err := m.dialer.Dial(mirror.workerAddr)
		if err != nil {
			return err
		}

		destInputConn, err := mux.Open(req.StdinId)
		if err != nil {
			return err
		}
		go func(rwc io.ReadWriteCloser) {
			HookedCopy(destInputConn, rwc, PrintStdinSliceLen)
			destInputConn.Close()
			rwc.Close()
		}(req.StdinConn)
		req.StdinConn = nil
	}

	log.Printf("Running task %d on %s: %v", req.TaskId, mirror.workerAddr, req.Argv)
	if req.Debug {
		log.Println("with environment", req.Env)
	}

	mirror.fileSetWaiter.Prepare(req.TaskId)
	m.mirrors.stats.Enter("remote")
	err = mirror.rpcClient.Call("Mirror.Run", req, rep)
	m.mirrors.stats.Exit("remote")
	if err == nil {
		m.mirrors.stats.Enter("filewait")
		err = mirror.fileSetWaiter.Wait(rep.FileSet, rep.TaskIds, req.TaskId)
		m.mirrors.stats.Exit("filewait")
	}
	return err
}

func (m *Master) runOnce(req *WorkRequest, rep *WorkResponse) error {
	mirror, err := m.mirrors.pick()
	if err != nil {
		return err
	}
	err = m.runOnMirror(mirror, req, rep)
	if err != nil {
		m.mirrors.drop(mirror, err)
		return err
	}

	return err
}

func (m *Master) run(req *WorkRequest, rep *WorkResponse) (err error) {
	m.mirrors.stats.Enter("run")
	defer m.mirrors.stats.Exit("run")
	req.TaskId = <-m.taskIds
	if m.MaybeRunInMaster(req, rep) {
		log.Println("Ran in master:", req.Summary())
		return nil
	}

	if req.Worker != "" {
		mc, err := m.mirrors.find(req.Worker)
		if err != nil {
			return err
		}
		return m.runOnMirror(mc, req, rep)
	}

	err = m.runOnce(req, rep)
	for i := 0; i < m.options.RetryCount && err != nil; i++ {
		log.Println("Retrying; last error:", err)
		err = m.runOnce(req, rep)
	}

	return err
}

func (m *Master) replayFileModifications(infos []*attr.FileAttr, delFileHashes map[string]string, newFiles map[string][]string) {
	for _, info := range infos {
		name := "/" + info.Path
		if info.Deletion() {
			if delFileHashes[info.Path] != "" {
				dest := fmt.Sprintf("%s/.termite-deltmp%x",
					m.options.WritableRoot, RandomBytes(8))
				if err := os.Rename(name, dest); err != nil {
					log.Fatal("os.Rename:", err)
				}
				newFiles[delFileHashes[info.Path]] = append(newFiles[delFileHashes[info.Path]], dest)
			} else {
				if err := os.Remove(name); err != nil {
					log.Fatal("os.Remove:", err)
				}
			}
			continue
		}

		if info.IsDir() {
			if err := os.Mkdir(name, os.FileMode(info.Mode&07777)); err != nil {
				// some other process may have created
				// the dir.
				fi, _ := os.Lstat(name)
				if fi == nil || !fi.IsDir() {
					log.Fatal("os.Mkdir", err)
				}
			}
		}
		if info.Hash != "" {
			fs := newFiles[info.Hash]
			src := fs[len(fs)-1]
			newFiles[info.Hash] = fs[:len(fs)-1]
			if err := os.Rename(src, name); err != nil {
				log.Fatal("os.Rename:", err)
			}
		}
		if info.Link != "" {
			// Ignore errors.
			os.Remove(name)
			if err := os.Symlink(info.Link, name); err != nil {
				log.Fatal("os.Symlink", err)
			}
		}
		if info.Hash == "" && !info.IsSymlink() {
			if err := os.Chtimes(name, info.AccessTime(), info.ModTime()); err != nil {
				log.Fatal("os.Chtimes", err)
			}
			if err := os.Chmod(name, os.FileMode(info.Mode&07777)); err != nil {
				log.Fatal("os.Chmod", err)
			}
		}

		// Reread FileInfo, since some filesystems (eg. ext3) do
		// not have nanosecond timestamps.
		//
		// TODO - test this.
		fi, _ := os.Lstat(name)
		info.Attr = fuse.ToAttr(fi)
		if info.IsRegular() && m.options.XAttrCache && info.Uid == uint32(m.options.Uid) {
			info.WriteXAttr(name)
		}
	}

	m.attributes.Update(infos)
	for _, v := range newFiles {
		for _, f := range v {
			if err := os.Remove(f); err != nil {
				log.Fatalf("os.Remove: %v", err)
			}
		}
	}
}

func (m *Master) replay(fset attr.FileSet) {
	// TODO - make a .termitetmp for replayed files.
	req := replayRequest{
		make(map[string][]string),
		make(map[string]string),
		fset.Files,
		make(chan int),
	}

	haveHashes := make(map[string]int)
	// We prepare the files before we call
	// replayFileModifications(), to limit contention.
	for _, info := range fset.Files {
		if info.Deletion() {
			a := m.attributes.Get(info.Path)
			if !a.Deletion() && a.Hash != "" {
				req.DelFileHashes[info.Path] = a.Hash
				haveHashes[a.Hash]++
			}
			continue
		}
		if info.Hash == "" {
			continue
		}
		if haveHashes[info.Hash] > 0 {
			haveHashes[info.Hash]--
			continue
		}

		log.Printf("Prepare %x: %s", info.Hash, info.Path)
		f, err := ioutil.TempFile(m.options.WritableRoot, ".tmp-termite")
		if err != nil {
			log.Fatal("TempFile", err)
		}

		req.NewFiles[info.Hash] = append(req.NewFiles[info.Hash], f.Name())

		var src *os.File
		path := m.contentStore.Path(info.Hash)
		src, err = os.Open(path)
		if err != nil {
			log.Panicf("cache path missing for %x: %v", info.Hash, err)
		}
		err = splice.CopyFds(f, src)
		src.Close()
		if err != nil {
			log.Fatal("f.CopyFds", err)
		}

		err = f.Chmod(os.FileMode(info.Attr.Mode & 07777))
		if err != nil {
			log.Fatal("f.Chmod", err)
		}
		err = f.Close()
		if err != nil {
			log.Fatal("f.Close", err)
		}
		err = os.Chtimes(f.Name(), info.AccessTime(), info.ModTime())
		if err != nil {
			log.Fatal("Chtimes", err)
		}
	}

	m.attributes.Queue(fset)

	m.replayChannel <- &req
	<-req.Done
}

func (m *Master) refreshAttributeCache() {
	updated := m.attributes.Refresh("")
	m.attributes.Queue(updated)
}

func (m *Master) fetchAll(path string) {
	a := m.attributes.GetDir(path)
	for n := range a.NameModeMap {
		m.fetchAll(filepath.Join(path, n))
	}
}

func (m *Master) waitForExit() {
	go m.mirrors.refreshWorkers()
	ticker := time.NewTicker(m.options.Period)

L:
	for {
		select {
		case <-m.quit:
			log.Println("quit received.")
			break L
		case <-ticker.C:
			log.Println("periodic household.")
			m.mirrors.periodicHouseholding()
		}
	}
}
