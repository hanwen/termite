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
	"strings"
)

type Master struct {
	cache         *ContentCache
	fileServer    *FsServer
	fileServerRpc *rpc.Server
	secret        []byte

	*localDecider

	stats          *masterStats
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
		stats:      newMasterStats(),
	}
	me.fileServer.multiplyPaths = func(n string) []string { return me.multiplyPaths(n) }
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

// TODO - write e2e test for this.
func (me *Master) multiplyPaths(name string) []string {
	names := []string{name}
	// TODO - cleanpath.
	if strings.HasPrefix(name, me.writableRoot) && me.srcRoot != "" {
		names = append(names, me.srcRoot+name[len(me.writableRoot):])
	}
	for _, n := range names {
		// TODO - configurable
		if strings.HasSuffix(n, ".gch") {
			names = append(names, n[:len(n)-len(".gch")])
		}
	}
	if len(names) > 1 {
		log.Println("multiplied", names)
	}
	return names
}

func (me *Master) SetSrcRoot(root string) {
	root, _ = filepath.Abs(root)
	me.srcRoot = filepath.Clean(root)
	log.Println("SrcRoot is", me.srcRoot)
}

func (me *Master) SetKeepAlive(seconds float64) {
	if seconds > 0 {
		me.mirrors.keepAliveNs = int64(1e9 * seconds)
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
			go func() {
				me.localRpcServer.ServeConn(conn)
				conn.Close()
			}()
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

	if err != nil {
		revConn.Close()
		rpcConn.Close()
		return nil, err
	}

	go func() {
		me.fileServerRpc.ServeConn(revConn)
		revConn.Close()
	}()

	mc := &mirrorConnection{
		rpcClient:     rpc.NewClient(rpcConn),
		connection:    rpcConn,
		maxJobs:       rep.GrantedJobCount,
		availableJobs: rep.GrantedJobCount,
	}

	mc.queueFiles(me.fileServer.copyCache())

	return mc, nil
}

func (me *Master) runOnMirror(mirror *mirrorConnection, req *WorkRequest, rep *WorkReply) os.Error {
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
	err := mirror.rpcClient.Call("Mirror.Run", &req, &rep)
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
		a := FileAttr{}
		me.fileServer.oneGetAttr(f, &a)
		req.Prefetch = append(req.Prefetch, a)
	}
	if len(req.Prefetch) > 0 {
		log.Println("Prefetch", req.Prefetch)
	}
}

func (me *Master) runOnce(req *WorkRequest, rep *WorkReply) os.Error {
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
	me.fileServer.updateFiles(localRep.Files)
	if err != nil {
		return err
	}
	*rep = localRep
	rep.Files = nil

	me.mirrors.queueFiles(mirror, localRep.Files)
	return err
}

func (me *Master) run(req *WorkRequest, rep *WorkReply) (err os.Error) {
	me.stats.MarkReceive()
	
	err = me.runOnce(req, rep)
	for i := 0; i < me.retryCount && err != nil; i++ {
		log.Println("Retrying; last error:", err)
		err = me.runOnce(req, rep)
	}

	me.stats.MarkReturn()
	return err
}

func (me *Master) replayFileModifications(worker *rpc.Client, infos []FileAttr) os.Error {
	// Must get data before we modify the file-system, so we don't
	// leave the FS in a half-finished state.
	for _, info := range infos {
		if info.Hash != "" && info.Content == nil {
			// TODO - stream directly from network connection to file.
			err := FetchBetweenContentServers(
				worker, "Mirror.FileContent", info.FileInfo.Size, info.Hash,
				me.cache)
			if err != nil {
				return err
			}
		}
	}

	entries := make(map[string]*FileAttr)
	names := []string{}
	for i, info := range infos {
		names = append(names, info.Path)
		entries[info.Path] = &infos[i]
	}

	deletes := []string{}
	// Sort so we get parents before children.
	sort.Strings(names)
	for _, name := range names {
		info := entries[name]
		var err os.Error

		// TODO - deletion test.
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
			content := info.Content

			if content == nil {
				err = CopyFile(info.Path, me.cache.Path(info.Hash), int(info.FileInfo.Mode))
				logStr += "CopyFile,"
			} else {
				me.cache.Save(content)
				err = ioutil.WriteFile(info.Path, content, info.FileInfo.Mode&07777)
				logStr += "WriteFile,"
			}
			if err == nil {
				err = os.Chtimes(info.Path, info.FileInfo.Atime_ns, info.FileInfo.Mtime_ns)
				logStr += "Chtimes,"
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

		if err != nil {
			log.Fatal("Replay error ", info.Path, " ", err, infos, logStr)
		}
	}

	// Must do deletes in reverse: children before parents.
	for i, _ := range deletes {
		d := deletes[len(deletes)-1-i]
		// TODO - should probably drop entries below d as well
		// if d is a directory.
		if err := os.RemoveAll(d); err != nil {
			log.Fatal("delete replay: ", err)
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

func (me *LocalMaster) Run(req *WorkRequest, rep *WorkReply) os.Error {
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
