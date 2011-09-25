package termite

import (
	"github.com/hanwen/go-fuse/fuse"
	"io/ioutil"
	"log"
	"net"
	"os"
	"path/filepath"
	"rpc"
	"sort"
)

type Master struct {
	cache         *ContentCache
	fileServer    *FsServer
	fileServerRpc *rpc.Server
	secret        []byte

	retryCount     int
	mirrors        *mirrorConnections
	localRpcServer *rpc.Server
	localServer    *LocalMaster
	writableRoot   string
	srcRoot        string
	pending        *PendingConnections
}

func NewMaster(cache *ContentCache, coordinator string, workers []string, secret []byte, excluded []string, maxJobs int) *Master {
	me := &Master{
		cache:      cache,
		fileServer: NewFsServer("/", cache, excluded),
		secret:     secret,
		retryCount: 3,
	}
	me.mirrors = newMirrorConnections(me, workers, coordinator, maxJobs)
	me.localServer = &LocalMaster{me}
	me.secret = secret
	me.pending = NewPendingConnections()
	me.fileServerRpc = rpc.NewServer()
	me.fileServerRpc.Register(me.fileServer)
	me.localRpcServer = rpc.NewServer()
	me.localRpcServer.Register(me.localServer)

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

func (me *Master) Start(sock string) {
	absSock, err := filepath.Abs(sock)
	if err != nil {
		log.Fatal("abs", err)
	}

	fi, _ := os.Stat(absSock)
	if fi != nil && fi.IsSocket() {
		conn, _ := net.Dial("unix", absSock)
		if conn != nil {
			conn.Close()
			log.Fatal("socket has someone listening: ", absSock)
		}
		// TODO - should check explicitly for the relevant error message.
		log.Println("removing dead socket", absSock)
		os.Remove(absSock)
	}

	listener, err := net.Listen("unix", absSock)
	defer os.Remove(absSock)
	if err != nil {
		log.Fatal("startLocalServer: ", err)
	}
	err = os.Chmod(absSock, 0700)
	if err != nil {
		log.Fatal("sock chmod", err)
	}

	me.writableRoot, err = filepath.EvalSymlinks(absSock)
	if err != nil {
		log.Fatal("EvalSymlinks", err)
	}
	me.writableRoot = filepath.Clean(me.writableRoot)
	me.writableRoot, _ = filepath.Split(me.writableRoot)
	me.writableRoot = filepath.Clean(me.writableRoot)

	log.Println("accepting connections on", absSock)
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Fatal("listener.accept", err)
		}
		if !me.pending.Accept(conn) {
			go me.localRpcServer.ServeConn(conn)
		}
	}
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

	err := mirror.rpcClient.Call("Mirror.Run", req, rep)
	rep.resetClock()
	return err
}

func (me *Master) prefetchFiles(req *WorkRequest) {
	files := map[string]int{}
	for _, arg := range req.Argv {
		for _, root := range []string{me.srcRoot, me.writableRoot} {
			for _, f := range DetectFiles(root, arg) {
				files[f] = 1
			}
		}
	}

	for f, _ := range files {
		a := me.fileServer.oneGetAttr(f)
		req.Prefetch = append(req.Prefetch, a)
	}
	if len(req.Prefetch) > 0 {
		log.Println("Prefetch", req.Prefetch)
	}
}

func (me *Master) runOnce(req *WorkRequest, rep *WorkResponse) os.Error {
	localRep := *rep
	mirror, err := me.mirrors.pick()
	if err != nil {
		return err
	}
	err = mirror.sendFiles()
	if err != nil {
		me.mirrors.drop(mirror, err)
		return err
	}

	me.prefetchFiles(req)
	err = me.runOnMirror(mirror, req, &localRep)
	if err != nil {
		me.mirrors.drop(mirror, err)
		return err
	}

	err = me.replayFileModifications(mirror.rpcClient, localRep.Files)
	localRep.clock("master.replay")
	me.fileServer.updateFiles(localRep.Files)
	localRep.clock("master.updateServer")
	if err != nil {
		return err
	}
	*rep = localRep
	rep.Files = nil

	me.mirrors.queueFiles(mirror, localRep.Files)
	rep.clock("master.queueFiles")
	return err
}

func (me *Master) run(req *WorkRequest, rep *WorkResponse) (err os.Error) {
	me.mirrors.stats.MarkReceive()

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
				worker, "Mirror.FileContent", info.FileInfo.Size, info.Hash,
				me.cache)
			if err != nil {
				return err
			}
		}
	}

	// TODO - if we have all readdir results in memory, we could
	// do the update of the FS asynchronous.
	entries := make(map[string]*FileAttr)
	names := []string{}
	for _, info := range infos {
		names = append(names, info.Path)
		entries[info.Path] = info
	}

	deletes := []string{}
	// Sort so we get parents before children.
	sort.Strings(names)
	for _, name := range names {
		info := entries[name]
		var err os.Error

		logStr := ""
		if info.FileInfo != nil && info.FileInfo.IsDirectory() {
			if name == "" {
				name = "/"
			}
			err = os.Mkdir(name, info.FileInfo.Mode&07777)
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
				err = CopyFile(info.Path, me.cache.Path(info.Hash), int(info.FileInfo.Mode))
				logStr += "CopyFile,"
			} else {
				me.cache.Save(content)
				err = ioutil.WriteFile(info.Path, content, info.FileInfo.Mode&07777)
				logStr += "WriteFile,"
			}
		}
		if info.Link != "" {
			os.Remove(info.Path) // ignore error.
			err = os.Symlink(info.Link, info.Path)
			logStr += "Symlink,"
		}
		if !info.Status.Ok() {
			if info.Status == fuse.ENOENT {
				deletes = append(deletes, info.Path)
			} else {
				log.Fatal("Unknown status for replay", info.Status)
			}
		}

		if info.FileInfo != nil && !info.FileInfo.IsSymlink() {
			if err == nil {
				err = os.Chtimes(info.Path, info.FileInfo.Atime_ns, info.FileInfo.Mtime_ns)
				logStr += "Chtimes,"
			}
			if err == nil {
				err = os.Chmod(info.Path, info.FileInfo.Mode&07777)
				logStr += "Chmod,"
			}
		}

		if err != nil {
			log.Fatal("Replay error ", info.Path, " ", err, infos, logStr)
		}
	}

	// Must do deletes in reverse: children before parents.
	for i, _ := range deletes {
		d := deletes[len(deletes)-1-i]
		if err := os.Remove(d); err != nil {
			log.Println("delete replay: ", err)
		}
	}
	return nil
}

func (me *Master) refreshAttributeCache() {
	for _, r := range []string{me.writableRoot, me.srcRoot} {
		updated := me.fileServer.refreshAttributeCache(r)
		me.mirrors.queueFiles(nil, updated)
	}
}

////////////////

// Expose functionality for the local tool to use.
type LocalMaster struct {
	master *Master
}

func (me *LocalMaster) Run(req *WorkRequest, rep *WorkResponse) os.Error {
	if req.RanLocally {
		log.Println("Ran command locally:", req.Argv)
		return nil
	}

	return me.master.run(req, rep)
}

func (me *LocalMaster) RefreshAttributeCache(input *int, output *int) os.Error {
	log.Println("Refreshing attribute cache")
	me.master.refreshAttributeCache()
	log.Println("Refresh done")
	return nil
}

func (me *LocalMaster) InspectFile(req *AttrRequest, rep *AttrResponse) os.Error {
	return me.master.fileServer.GetAttr(req, rep)
}
