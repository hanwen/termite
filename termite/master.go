package termite

import (
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"strings"
)

type Master struct {
	cache         *ContentCache
	fileServer    *FsServer
	fileServerRpc *rpc.Server
	excluded      map[string]bool
	attr          *AttributeCache
	mirrors       *mirrorConnections
	pending       *PendingConnections
	taskIds       chan int
	options       *MasterOptions
	replayChannel chan *replayRequest
}

type MasterOptions struct {
	WritableRoot   string
	SrcRoot        string
	RetryCount     int
	Excludes       []string
	ExcludePrivate bool

	Coordinator string
	Workers     []string
	Secret      []byte
	MaxJobs     int
	Paranoia    bool
}

type replayRequest struct {
	NewFiles map[string]string
	Files    []*FileAttr
	Done     chan int
}

func (me *Master) uncachedGetAttr(name string) (rep *FileAttr) {
	rep = &FileAttr{Path: name}
	p := me.path(name)
	fi, _ := os.Lstat(p)

	// We don't want to expose the master's private files to the
	// world.
	if me.options.ExcludePrivate && fi != nil && fi.Mode&0077 == 0 {
		log.Printf("Denied access to private file %q", name)
		return rep
	}

	if me.excluded[name] {
		log.Printf("Denied access to excluded file %q", name)
		return rep
	}
	rep.FileInfo = fi
	if fi != nil {
		me.fillContent(rep)
	}
	return rep
}

func (me *Master) fillContent(rep *FileAttr) {
	if rep.IsSymlink() || rep.IsDirectory() {
		rep.ReadFromFs(me.path(rep.Path))
	} else if rep.IsRegular() {
		fullPath := me.path(rep.Path)
		rep.Hash = me.cache.SavePath(fullPath)
		if rep.Hash == "" {
			// Typically happens if we want to open /etc/shadow as normal user.
			log.Println("fillContent returning EPERM for", rep.Path)
			rep.FileInfo = nil
		}
	}
}

func (me *Master) path(n string) string {
	return "/" + n
}

func NewMaster(cache *ContentCache, options *MasterOptions) *Master {
	me := &Master{
		cache:         cache,
		taskIds:       make(chan int, 100),
		replayChannel: make(chan *replayRequest, 1),
	}
	o := *options
	me.options = &o
	me.excluded = make(map[string]bool)
	for _, e := range options.Excludes {
		me.excluded[e] = true
	}

	me.mirrors = newMirrorConnections(
		me, options.Workers, options.Coordinator, options.MaxJobs)
	me.pending = NewPendingConnections()
	me.attr = NewAttributeCache(func(n string) *FileAttr {
		return me.uncachedGetAttr(n)
	},
		func(n string) *os.FileInfo {
			fi, _ := os.Lstat(me.path(n))
			return fi
		})
	me.fileServer = NewFsServer(me.attr, me.cache)
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
			me.replayFileModifications(r.Files, r.NewFiles)
			r.Done <- 1
		}
	}()

	return me
}

// todo - move to options.
func (me *Master) SetKeepAlive(keepalive float64, household float64) {
	if household > 0.0 || keepalive > 0.0 {
		me.mirrors.setKeepAliveNs(1e9*keepalive, 1e9*household)
	}
}

func (me *Master) CheckPrivate() {
	if !me.options.ExcludePrivate {
		return
	}
	d := me.options.WritableRoot
	for d != "" {
		fi, err := os.Lstat(d)
		if err != nil {
			log.Fatal("CheckPrivate:", err)
		}
		if fi != nil && fi.Mode&0077 == 0 {
			log.Fatalf("Error: dir %q is mode %o.", d, fi.Mode&07777)
		}
		d, _ = SplitPath(d)
	}
}

func (me *Master) Start(sock string) {
	// Fetch in the background.
	last := ""
	for _, r := range []string{me.options.WritableRoot, me.options.SrcRoot} {
		if last == r || r == "" {
			continue
		}
		go me.fetchAll(strings.TrimLeft(r, "/"))
	}
	localStart(me, sock)
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
	mc.fileSetWaiter = newFileSetWaiter(func(fset FileSet) error {
		return mc.replay(fset)
	})

	return mc, nil
}

func (me *Master) runOnMirror(mirror *mirrorConnection, req *WorkRequest, rep *WorkResponse) error {
	me.mirrors.stats.Enter("send")
	err := me.fileServer.attr.Send(mirror)
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

	mirror.fileSetWaiter.newChannel(req.TaskId)
	me.mirrors.stats.Enter("remote")
	err = mirror.rpcClient.Call("Mirror.Run", req, rep)
	me.mirrors.stats.Exit("remote")
	if err == nil {
		me.mirrors.stats.Enter("filewait")
		err = mirror.fileSetWaiter.wait(rep, req.TaskId)
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

func (me *Master) replayFileModifications(infos []*FileAttr, newFiles map[string]string) {
	for _, info := range infos {
		logStr := ""
		name := "/" + info.Path
		var err error
		if info.FileInfo != nil && info.FileInfo.IsDirectory() {
			err := os.Mkdir(name, info.FileInfo.Mode&07777)
			if err != nil {
				// some other process may have created
				// the dir.
				if fi, _ := os.Lstat(name); fi != nil && fi.IsDirectory() {
					err = nil
				}
			}
		}
		if info.Hash != "" {
			src := newFiles[info.Path]
			err = os.Rename(src, name)
		}
		if info.Link != "" {
			// Ignore errors.
			os.Remove(name)
			err = os.Symlink(info.Link, name)
			logStr += "Symlink,"
		}
		if info.Deletion() {
			if err := os.Remove(name); err != nil {
				log.Println("delete replay: ", err)
			}
		}

		if info.Hash == "" && info.FileInfo != nil && !info.FileInfo.IsSymlink() {
			if err == nil {
				err = os.Chtimes(name, info.FileInfo.Atime_ns, info.FileInfo.Mtime_ns)
				logStr += "Chtimes,"
			}
			if err == nil {
				err = os.Chmod(name, info.FileInfo.Mode&07777)
				logStr += "Chmod,"
			}
		}

		if err != nil {
			log.Fatal("Replay error ", info.Path, " ", err, infos, logStr)
		}
	}

	me.attr.Update(infos)
}

func (me *Master) replay(fset FileSet) {
	req := replayRequest{
		make(map[string]string),
		fset.Files,
		make(chan int),
	}

	// We prepare the files before we call
	// replayFileModifications(), to limit contention.
	for _, info := range fset.Files {
		if info.Hash == "" {
			continue
		}

		log.Printf("Prepare %x: %s", info.Hash, info.Path)
		f, err := ioutil.TempFile(me.options.WritableRoot, ".tmp-termite")
		if err != nil {
			log.Fatal("TempFile", err)
		}

		req.NewFiles[info.Path] = f.Name()

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

		err = f.Chmod(info.FileInfo.Mode & 07777)
		if err != nil {
			log.Fatal("f.Chmod", err)
		}
		err = f.Close()
		if err != nil {
			log.Fatal("f.Close", err)
		}
		err = os.Chtimes(f.Name(), info.FileInfo.Atime_ns, info.FileInfo.Mtime_ns)
		if err != nil {
			log.Fatal("Chtimes", err)
		}
	}

	me.fileServer.attr.Queue(fset)

	me.replayChannel <- &req
	<-req.Done
}

func (me *Master) refreshAttributeCache() {
	last := ""
	for _, r := range []string{me.options.WritableRoot, me.options.SrcRoot} {
		if last == r || r == "" {
			continue
		}
		updated := me.attr.Refresh(r[1:])
		me.attr.Queue(updated)
		last = r
	}
}

func (me *Master) fetchAll(path string) {
	a := me.attr.GetDir(path)
	for n := range a.NameModeMap {
		me.fetchAll(filepath.Join(path, n))
	}
}
