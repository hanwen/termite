package termite

import (
	"fmt"
	"github.com/hanwen/go-fuse/fuse"
	"github.com/hanwen/termite/attr"
	"github.com/hanwen/termite/cba"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

type Master struct {
	cache         *cba.ContentCache
	fileServer    *FsServer
	fileServerRpc *rpc.Server
	excluded      map[string]bool
	attributes    *attr.AttributeCache
	mirrors       *mirrorConnections
	pending       *PendingConnections
	taskIds       chan int
	options       *MasterOptions
	replayChannel chan *replayRequest
	quit          chan int
}

// Immutable state and options for master.
type MasterOptions struct {
	cba.ContentCacheOptions

	WritableRoot  string
	SourceRoot       string

	// How often a failed should be retried on another worker.
	RetryCount    int

	// List of files that should not be served
	Excludes      []string

	// If set, also serve files that have no group/other permissions
	ExposePrivate bool

	// Address of the coordinator.
	Coordinator string

	Secret      []byte
	
	MaxJobs     int

	// Turns on internal consistency checks. Expensive.
	Paranoia    bool

	// On startup, fault-in all files.
	FetchAll bool

	// How often to do periodic householding work.
	Period    time.Duration

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

func (me *Master) uncachedGetAttr(name string) (rep *attr.FileAttr) {
	rep = &attr.FileAttr{Path: name}
	p := me.path(name)

	if p == me.options.Socket || p == me.options.LogFile {
		return rep
	}

	fi, _ := os.Lstat(p)

	// We don't want to expose the master's private files to the
	// world.
	if !me.options.ExposePrivate && fi != nil && fi.Mode().Perm()&0077 == 0 {
		log.Printf("Denied access to private file %q", name)
		return rep
	}

	if me.excluded[name] {
		log.Printf("Denied access to excluded file %q", name)
		return rep
	}
	rep.Attr = fuse.ToAttr(fi)

	xattrPossible := rep.IsRegular() && me.options.XAttrCache && rep.Uid == uint32(me.options.Uid) && ((me.options.WritableRoot != "" && strings.HasPrefix(p, me.options.WritableRoot)) ||
		(me.options.SourceRoot != "" && strings.HasPrefix(p, me.options.SourceRoot)))

	if xattrPossible {
		cur := attr.EncodedAttr{}
		cur.FromAttr(rep.Attr)

		disk := attr.EncodedAttr{}
		diskHash := disk.ReadXAttr(p)

		if diskHash != nil && cur.Eq(&disk) && me.cache.HasHash(string(diskHash)) {
			rep.Hash = string(diskHash)
			return rep
		}
	}

	if fi != nil {
		me.fillContent(rep)
		if xattrPossible {
			if rep.Mode&0222 == 0 {
				os.Chmod(p, rep.Mode|0200)
			}
			rep.WriteXAttr(p)
			if rep.Mode&0222 == 0 {
				os.Chmod(p, rep.Mode)
			}
		}
	}
	return rep
}

func (me *Master) fillContent(rep *attr.FileAttr) {
	if rep.IsSymlink() || rep.IsDir() {
		rep.ReadFromFs(me.path(rep.Path), me.options.Hash)
	} else if rep.IsRegular() {
		fullPath := me.path(rep.Path)
		rep.Hash = me.cache.SavePath(fullPath)
		if rep.Hash == "" {
			// Typically happens if we want to open /etc/shadow as normal user.
			log.Println("fillContent returning EPERM for", rep.Path)
			rep.Attr = nil
		}
	}
}

func (me *Master) path(n string) string {
	return "/" + n
}

func NewMaster(options *MasterOptions) *Master {
	cache := cba.NewContentCache(&options.ContentCacheOptions)

	me := &Master{
		cache:         cache,
		taskIds:       make(chan int, 100),
		replayChannel: make(chan *replayRequest, 1),
		quit:          make(chan int, 0),
	}
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

	me.options = &o
	me.excluded = make(map[string]bool)
	for _, e := range options.Excludes {
		me.excluded[e] = true
	}

	me.mirrors = newMirrorConnections(
		me, options.Coordinator, options.MaxJobs)
	me.mirrors.keepAlive = options.KeepAlive
	me.pending = NewPendingConnections()
	me.attributes = attr.NewAttributeCache(func(n string) *attr.FileAttr {
		return me.uncachedGetAttr(n)
	},
		func(n string) *fuse.Attr {
			fi, _ := os.Lstat(me.path(n))
			return fuse.ToAttr(fi)
		})
	me.fileServer = NewFsServer(me.attributes, me.cache)
	me.fileServerRpc = rpc.NewServer()
	me.fileServerRpc.Register(me.fileServer)

	me.CheckPrivate()

	// Generate taskids.
	go func() {
		i := 0
		for {
			me.taskIds <- i
			i++
		}
	}()

	// Make sure we update the filesystem and attributes together.
	go func() {
		for {
			r := <-me.replayChannel
			me.replayFileModifications(r.Files, r.DelFileHashes, r.NewFiles)
			r.Done <- 1
		}
	}()

	return me
}

func (me *Master) CheckPrivate() {
	if me.options.ExposePrivate {
		return
	}
	d := me.options.WritableRoot
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
func (me *Master) FetchAll() {
	wg := sync.WaitGroup{}
	last := ""
	for _, r := range []string{me.options.WritableRoot, me.options.SourceRoot} {
		if last == r || r == "" {
			continue
		}
		last = r
		wg.Add(1)

		go func(p string) {
			log.Println("Prefetch", p)
			me.fetchAll(strings.TrimLeft(p, "/"))
			wg.Done()
		}(r)
	}
	wg.Wait()
	log.Println("FetchAll done")
}

func (me *Master) Start() {
	if me.options.FetchAll {
		go me.FetchAll()
	}
	go localStart(me, me.options.Socket)
	me.waitForExit()
}

func (me *Master) createMirror(addr string, jobs int) (*mirrorConnection, error) {
	secret := me.options.Secret
	conn, err := DialTypedConnection(addr, RPC_CHANNEL, secret)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	rpcId := ConnectionId()
	rpcConn, err := DialTypedConnection(addr, rpcId, secret)
	if err != nil {
		return nil, err
	}

	revId := ConnectionId()
	revConn, err := DialTypedConnection(addr, revId, secret)
	if err != nil {
		rpcConn.Close()
		return nil, err
	}

	req := CreateMirrorRequest{
		RpcId:        rpcId,
		RevRpcId:     revId,
		WritableRoot: me.options.WritableRoot,
		MaxJobCount:  jobs,
	}
	rep := CreateMirrorResponse{}
	cl := rpc.NewClient(conn)
	err = cl.Call("Worker.CreateMirror", &req, &rep)
	cl.Close()

	if err != nil {
		revConn.Close()
		rpcConn.Close()
		return nil, err
	}

	go me.fileServerRpc.ServeConn(revConn)

	mc := &mirrorConnection{
		master:            me,
		rpcClient:         rpc.NewClient(rpcConn),
		reverseConnection: revConn,
		connection:        rpcConn,
		maxJobs:           rep.GrantedJobCount,
		availableJobs:     rep.GrantedJobCount,
	}
	mc.fileSetWaiter = attr.NewFileSetWaiter(func(fset attr.FileSet) error {
		return mc.replay(fset)
	})

	return mc, nil
}

func (me *Master) runOnMirror(mirror *mirrorConnection, req *WorkRequest, rep *WorkResponse) error {
	me.mirrors.stats.Enter("send")
	err := me.fileServer.attributes.Send(mirror)
	me.mirrors.stats.Exit("send")
	if err != nil {
		return err
	}

	defer me.mirrors.jobDone(mirror)

	// Tunnel stdin.
	if req.StdinId != "" {
		inputConn := me.pending.WaitConnection(req.StdinId)
		destInputConn, err := DialTypedConnection(mirror.connection.RemoteAddr().String(),
			req.StdinId, me.options.Secret)
		if err != nil {
			return err
		}
		go func() {
			HookedCopy(destInputConn, inputConn, PrintStdinSliceLen)
			destInputConn.Close()
			inputConn.Close()
		}()
	}

	log.Printf("Running task %d on %s: %v", req.TaskId, mirror.workerAddr, req.Argv)
	if req.Debug {
		log.Println("with environment", req.Env)
	}

	mirror.fileSetWaiter.Prepare(req.TaskId)
	me.mirrors.stats.Enter("remote")
	err = mirror.rpcClient.Call("Mirror.Run", req, rep)
	me.mirrors.stats.Exit("remote")
	if err == nil {
		me.mirrors.stats.Enter("filewait")
		err = mirror.fileSetWaiter.Wait(rep.FileSet, rep.TaskIds, req.TaskId)
		me.mirrors.stats.Exit("filewait")
	}
	return err
}

func (me *Master) runOnce(req *WorkRequest, rep *WorkResponse) error {
	mirror, err := me.mirrors.pick()
	if err != nil {
		return err
	}
	err = me.runOnMirror(mirror, req, rep)
	if err != nil {
		me.mirrors.drop(mirror, err)
		return err
	}

	rep.FileSet = nil
	return err
}

func (me *Master) run(req *WorkRequest, rep *WorkResponse) (err error) {
	me.mirrors.stats.Enter("run")
	defer me.mirrors.stats.Exit("run")
	req.TaskId = <-me.taskIds
	if me.MaybeRunInMaster(req, rep) {
		log.Println("Ran in master:", req.Summary())
		return nil
	}

	if req.Worker != "" {
		mc, err := me.mirrors.find(req.Worker)
		if err != nil {
			return err
		}
		return me.runOnMirror(mc, req, rep)
	}

	err = me.runOnce(req, rep)
	for i := 0; i < me.options.RetryCount && err != nil; i++ {
		log.Println("Retrying; last error:", err)
		err = me.runOnce(req, rep)
	}

	return err
}

func (me *Master) replayFileModifications(infos []*attr.FileAttr, delFileHashes map[string]string, newFiles map[string][]string) {
	for _, info := range infos {
		name := "/" + info.Path
		if info.Deletion() {
			if delFileHashes[info.Path] != "" {
				dest := fmt.Sprintf("%s/.termite-deltmp%x",
					me.options.WritableRoot, RandomBytes(8))
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
			if err := os.Mkdir(name, info.Mode&07777); err != nil {
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
			if err := os.Chmod(name, info.Mode&07777); err != nil {
				log.Fatal("os.Chmod", err)
			}
		}

		// Reread FileInfo, since some filesystems (eg. ext3) do
		// not have nanosecond timestamps.
		//
		// TODO - test this.
		fi, _ := os.Lstat(name)
		info.Attr = fuse.ToAttr(fi)
		if info.IsRegular() && me.options.XAttrCache && info.Uid == uint32(me.options.Uid) {
			info.WriteXAttr(name)
		}
	}

	me.attributes.Update(infos)
	for _, v := range newFiles {
		for _, f := range v {
			if err := os.Remove(f); err != nil {
				log.Fatalf("os.Remove: %v", err)
			}
		}
	}
}

func (me *Master) replay(fset attr.FileSet) {
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
			a := me.attributes.Get(info.Path)
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
		f, err := ioutil.TempFile(me.options.WritableRoot, ".tmp-termite")
		if err != nil {
			log.Fatal("TempFile", err)
		}

		req.NewFiles[info.Hash] = append(req.NewFiles[info.Hash], f.Name())
		content := me.cache.ContentsIfLoaded(info.Hash)

		if content == nil {
			var src *os.File
			src, err = os.Open(me.cache.Path(info.Hash))
			if err != nil {
				log.Panicf("cache path missing %x", info.Hash)
			}
			err = CopyFds(f, src)
		} else {
			_, err = f.Write(content)
		}
		if err != nil {
			log.Fatal("f.Write", err)
		}

		err = f.Chmod(info.Attr.Mode & 07777)
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

	me.fileServer.attributes.Queue(fset)

	me.replayChannel <- &req
	<-req.Done
}

func (me *Master) refreshAttributeCache() {
	updated := me.attributes.Refresh("")
	me.attributes.Queue(updated)
}

func (me *Master) fetchAll(path string) {
	a := me.attributes.GetDir(path)
	for n := range a.NameModeMap {
		me.fetchAll(filepath.Join(path, n))
	}
}

func (me *Master) waitForExit() {
	me.mirrors.refreshWorkers()
	ticker := time.NewTicker(me.options.Period)

L:
	for {
		select {
		case <-me.quit:
			log.Println("quit received.")
			break L
		case <-ticker.C:
			log.Println("periodic household.")
			me.mirrors.periodicHouseholding()
		}
	}
}
