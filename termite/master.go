package termite

import (
	"io/ioutil"
	"log"
	"net"
	"os"
	"path/filepath"
	"github.com/hanwen/go-fuse/fuse"
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

	pending *PendingConnections
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

func (me *Master) SetKeepAlive(seconds int64) {
	if seconds > 0 {
		me.mirrors.keepAliveSeconds = seconds
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
		// TODO - should explicitly for the relevant error message.
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

	rpcId := ConnectionId()
	rpcConn, err := DialTypedConnection(addr, rpcId, me.secret)
	if err != nil {
		conn.Close()
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

	go me.fileServerRpc.ServeConn(revConn)

	return &mirrorConnection{
		rpcClient:     rpc.NewClient(rpcConn),
		connection:    rpcConn,
		maxJobs:       rep.MaxJobCount,
		availableJobs: rep.MaxJobCount,
	}, nil
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

	err := mirror.rpcClient.Call("Mirror.Run", &req, &rep)
	return err
}

func (me *Master) runOnce(req *WorkRequest, rep *WorkReply) os.Error {
	localRep := *rep
	mirror, err := me.mirrors.pick()
	if err != nil {
		return err
	}

	err = me.runOnMirror(mirror, req, &localRep)
	if err != nil {
		me.mirrors.drop(mirror, err)
		return err
	}

	me.fileServer.updateHashes(localRep.Files)
	err = me.replayFileModifications(mirror.rpcClient, localRep.Files)
	if err != nil {
		return err
	}
	*rep = localRep
	rep.Files = nil

	// This must happen synchronously, otherwise, other mirrors
	// may see their files change mid-compile.
	me.mirrors.broadcastFiles(mirror, localRep.Files)
	return err
}

func (me *Master) run(req *WorkRequest, rep *WorkReply) (err os.Error) {
	err = me.runOnce(req, rep)
	for i := 0; i < me.retryCount && err != nil; i++ {
		log.Println("Retrying; last error:", err)
		err = me.runOnce(req, rep)
	}
	return err
}

func (me *Master) replayFileModifications(worker *rpc.Client, infos []AttrResponse) os.Error {
	// Must get data before we modify the file-system, so we don't
	// leave the FS in a half-finished state.
	for _, info := range infos {
		if info.Hash != nil && info.Content == nil {
			// TODO - stream directly from network connection to file.
			err := FetchBetweenContentServers(
				worker, "Mirror.FileContent", info.FileInfo.Size, info.Hash,
				me.cache)
			if err != nil {
				return err
			}
		}
	}

	entries := make(map[string]*AttrResponse)
	names := []string{}
	for i, info := range infos {
		names = append(names, info.Path)
		entries[info.Path] = &infos[i]
	}

	// Sort so we get parents before children.
	sort.Strings(names)
	for _, name := range names {
		info := entries[name]
		var err os.Error
		// TODO - deletion test.
		if info.FileInfo != nil && info.FileInfo.IsDirectory() {
			if name == "" {
				name = "/"
			}
			err = os.Mkdir(name, info.FileInfo.Mode&07777)
			if err != nil {
				// some other process may have created
				// the dir.
				if fi, _ := os.Lstat(name); fi != nil {
					err = nil
				}
			}
		}
		if info.Hash != nil {
			log.Printf("Replay file content %s %x", name, info.Hash)
			content := info.Content

			if content == nil {
				err = CopyFile(info.Path, me.cache.Path(info.Hash), int(info.FileInfo.Mode))
			} else {
				me.cache.Save(content)
				err = ioutil.WriteFile(info.Path, content, info.FileInfo.Mode&07777)
			}
			if err == nil {
				err = os.Chtimes(info.Path, info.FileInfo.Atime_ns, info.FileInfo.Mtime_ns)
			}
		}
		if info.Link != "" {
			log.Println("Replay symlink:", name)
			err = os.Symlink(info.Link, info.Path)
		}
		if !info.Status.Ok() {
			if info.Status == fuse.ENOENT {
				log.Println("Replay delete:", name)
				err = os.Remove(info.Path)
			} else {
				log.Fatal("Unknown status for replay", info.Status)
			}
		}

		if err != nil {
			log.Fatal("Replay error ", info.Path, " ", err)
		}
	}
	return nil
}

////////////////

// Expose functionality for the local tool to use.
type LocalMaster struct {
	master *Master
}

func (me *LocalMaster) Run(req *WorkRequest, rep *WorkReply) os.Error {
	return me.master.run(req, rep)
}
