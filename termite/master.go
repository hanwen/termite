package termite

import (
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"rpc"
)

type Master struct {
	cache         *ContentCache
	fileServer    *FsServer
	fileServerRpc *rpc.Server
	secret        []byte

	retryCount   int
	mirrors      *mirrorConnections
	writableRoot string
	srcRoot      string
	pending      *PendingConnections
	taskIds      chan int
}

func NewMaster(cache *ContentCache, coordinator string, workers []string, secret []byte, excluded []string, maxJobs int) *Master {
	me := &Master{
		cache:      cache,
		fileServer: NewFsServer("/", cache, excluded),
		secret:     secret,
		retryCount: 3,
		taskIds:    make(chan int, 100),
	}
	me.mirrors = newMirrorConnections(me, workers, coordinator, maxJobs)
	me.secret = secret
	me.pending = NewPendingConnections()
	me.fileServerRpc = rpc.NewServer()
	me.fileServerRpc.Register(me.fileServer)

	go func() {
		i := 0
		for {
			me.taskIds <-i
			i++
		}
	}()
	
	return me
}

func (me *Master) SetSrcRoot(root string) {
	root, _ = filepath.Abs(root)
	me.srcRoot = filepath.Clean(root)
	log.Println("SrcRoot is", me.srcRoot)
}

func (me *Master) SetKeepAlive(keepalive float64, household float64) {
	if household > 0.0 || keepalive > 0.0 {
		me.mirrors.setKeepAliveNs(1e9*keepalive, 1e9*household)
	}
}

func (me *Master) CheckPrivate() {
	if !me.fileServer.excludePrivate {
		return
	}
	d := me.writableRoot
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
	localStart(me, sock)
}

func (me *Master) createMirror(addr string, jobs int) (*mirrorConnection, os.Error) {
	conn, err := DialTypedConnection(addr, RPC_CHANNEL, me.secret)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	rpcId := ConnectionId()
	rpcConn, err := DialTypedConnection(addr, rpcId, me.secret)
	if err != nil {
		return nil, err
	}

	revId := ConnectionId()
	revConn, err := DialTypedConnection(addr, revId, me.secret)
	if err != nil {
		rpcConn.Close()
		return nil, err
	}

	req := CreateMirrorRequest{
		RpcId:        rpcId,
		RevRpcId:     revId,
		WritableRoot: me.writableRoot,
		MaxJobCount:  jobs,
	}
	rep := CreateMirrorResponse{}
	cl := rpc.NewClient(conn)
	err = cl.Call("WorkerDaemon.CreateMirror", &req, &rep)
	cl.Close()

	if err != nil {
		revConn.Close()
		rpcConn.Close()
		return nil, err
	}

	go me.fileServerRpc.ServeConn(revConn)

	mc := &mirrorConnection{
		rpcClient:         rpc.NewClient(rpcConn),
		reverseConnection: revConn,
		connection:        rpcConn,
		maxJobs:           rep.GrantedJobCount,
		availableJobs:     rep.GrantedJobCount,
	}
	mc.fileSetWaiter = newFileSetWaiter(me, mc)

	mc.queueFiles(me.fileServer.copyCache())
	return mc, nil
}

func (me *Master) runOnMirror(mirror *mirrorConnection, req *WorkRequest, rep *WorkResponse) os.Error {
	defer me.mirrors.jobDone(mirror)

	// Tunnel stdin.
	if req.StdinId != "" {
		inputConn := me.pending.WaitConnection(req.StdinId)
		destInputConn, err := DialTypedConnection(mirror.connection.RemoteAddr().String(),
			req.StdinId, me.secret)
		if err != nil {
			return err
		}
		go func() {
			HookedCopy(destInputConn, inputConn, PrintStdinSliceLen)
			destInputConn.Close()
			inputConn.Close()
		}()
	}

	log.Println("Running command", req.Argv)
	if req.Debug {
		log.Println("with environment", req.Env)
	}

	mirror.fileSetWaiter.newChannel(req.TaskId)
	err := mirror.rpcClient.Call("Mirror.Run", req, rep)
	if err == nil {
		err = mirror.fileSetWaiter.wait(rep, req.TaskId)
	}
	return err
}

func (me *Master) runOnce(req *WorkRequest, rep *WorkResponse) os.Error {
	mirror, err := me.mirrors.pick()
	if err != nil {
		return err
	}
	err = mirror.sendFiles()
	if err != nil {
		me.mirrors.drop(mirror, err)
		return err
	}

	err = me.runOnMirror(mirror, req, rep)
	if err != nil {
		me.mirrors.drop(mirror, err)
		return err
	}

	if err != nil {
		return err
	}
	rep.FileSet = nil
	return err
}

func (me *Master) run(req *WorkRequest, rep *WorkResponse) (err os.Error) {
	me.mirrors.stats.MarkReceive()
	req.TaskId = <-me.taskIds

	err = me.runOnce(req, rep)
	for i := 0; i < me.retryCount && err != nil; i++ {
		log.Println("Retrying; last error:", err)
		err = me.runOnce(req, rep)
	}

	me.mirrors.stats.MarkReturn(rep)
	return err
}

func (me *Master) replayFileModifications(worker *rpc.Client, infos []*FileAttr) os.Error {
	// Must get data before we modify the file-system, so we don't
	// leave the FS in a half-finished state.
	for _, info := range infos {
		if info.Hash != "" {
			err := FetchBetweenContentServers(
				worker, "Mirror.FileContent", info.Hash, me.cache)
			if err != nil {
				return err
			}
		}
	}

	// TODO - if we have all readdir results in memory, we could
	// do the update of the FS asynchronous.
	for _, info := range infos {
		logStr := ""
		name := "/" + info.Path
		var err os.Error
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
			log.Printf("Replay file content %s %x", name, info.Hash)
			content := me.cache.ContentsIfLoaded(info.Hash)

			if content == nil {
				err = CopyFile(name, me.cache.Path(info.Hash), int(info.FileInfo.Mode))
				logStr += "CopyFile,"
			} else {
				me.cache.Save(content)
				err = ioutil.WriteFile(name, content, info.FileInfo.Mode&07777)
				logStr += "WriteFile,"
			}
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

		if info.FileInfo != nil && !info.FileInfo.IsSymlink() {
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

	return nil
}

func (me *Master) refreshAttributeCache() {
	for _, r := range []string{me.writableRoot, me.srcRoot} {
		updated := me.fileServer.refreshAttributeCache(r[1:])
		me.mirrors.queueFiles(nil, updated)
	}
}
