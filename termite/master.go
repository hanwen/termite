package termite

import (
	"bytes"
	"io/ioutil"
	"log"
	"net"
	"os"
	"path/filepath"
	"github.com/hanwen/go-fuse/fuse"
	"rand"
	"rpc"
	"sort"
	"sync"
)

type mirrorConnection struct {
	rpcClient  *rpc.Client
	connection net.Conn
}

type Master struct {
	cache         *DiskFileCache
	fileServer    *FsServer
	fileServerRpc *rpc.Server
	secret        []byte

	workServers []string

	mirrorsMutex sync.Mutex
	mirrors      []mirrorConnection

	localRpcServer *rpc.Server
	localServer    *LocalMaster
	writableRoot   string

	pending *PendingConnections
}

func NewMaster(cacheDir string, workers []string, secret []byte, excluded []string) *Master {
	c := NewDiskFileCache(cacheDir)
	me := &Master{
		cache:      c,
		fileServer: NewFsServer("/", c, excluded),
		secret:     secret,
	}
	me.localServer = &LocalMaster{me}
	me.workServers = workers
	me.secret = secret
	me.pending = NewPendingConnections()
	me.fileServerRpc = rpc.NewServer()
	me.fileServerRpc.Register(me.fileServer)

	me.localRpcServer = rpc.NewServer()
	me.localRpcServer.Register(me.localServer)
	return me
}

func (me *Master) listenLocal(sock string) {
	absSock, err := filepath.Abs(sock)
	if err != nil {
		log.Fatal("abs", err)
	}

	listener, err := net.Listen("unix", absSock)
	defer os.Remove(absSock)
	if err != nil {
		log.Fatal("startLocalServer", err)
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

	log.Println("Accepting connections on", absSock)
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Fatal("listener.accept", err)
		}
		me.pending.Accept(conn)
	}
}

func (me *Master) Start(sock string) {
	go me.listenLocal(sock)

	for {
		conn := me.pending.WaitConnection(RPC_CHANNEL)
		go me.localRpcServer.ServeConn(conn)
	}
}

func (me *Master) createMirror(addr string) (net.Conn, os.Error) {
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
	return rpcConn, nil
}

func (me *Master) createMirrors() os.Error {
	me.mirrorsMutex.Lock()
	defer me.mirrorsMutex.Unlock()

	// TODO - check if they are alive.
	if len(me.mirrors) > 0 {
		return nil
	}

	out := make(chan net.Conn, 1)
	for _, addr := range me.workServers {
		go func(a string) {
			conn, err := me.createMirror(a)
			if err != nil {
				log.Println("nonfatal", err)
			}
			out <- conn
		}(addr)
	}

	for _, _ = range me.workServers {
		c := <-out
		if c != nil {
			mc := mirrorConnection{
				rpc.NewClient(c),
				c,
			}
			me.mirrors = append(me.mirrors, mc)
		}
	}

	if len(me.mirrors) == 0 {
		return os.NewError("No workers available")
	}
	return nil
}

func (me *Master) run(req *WorkRequest, rep *WorkReply) os.Error {
	err := me.createMirrors()
	if err != nil {
		return err
	}

	idx := rand.Intn(len(me.mirrors))

	// Tunnel stdin.
	inputConn := me.pending.WaitConnection(req.StdinId)
	destInputConn, err := DialTypedConnection(me.mirrors[idx].connection.RemoteAddr().String(),
		req.StdinId, me.secret)
	if err != nil {
		return err
	}

	go func() {
		HookedCopy(destInputConn, inputConn, PrintStdinSliceLen)
		destInputConn.Close()
		inputConn.Close()
	}()

	mirror := me.mirrors[idx].rpcClient
	localRep := *rep
	err = mirror.Call("Mirror.Run", &req, &localRep)
	if err != nil {
		return err
	}
	me.replayFileModifications(mirror, localRep.Files)
	*rep = localRep
	rep.Files = nil

	go me.broadcastFiles(mirror, localRep.Files)
	return err
}

func (me *Master) broadcastFiles(origin *rpc.Client, infos []AttrResponse) {
	for _, w := range me.mirrors {
		if origin != w.rpcClient {
			go me.broadcastFilesTo(w.rpcClient, infos)
		}
	}
}

func (me *Master) broadcastFilesTo(worker *rpc.Client, infos []AttrResponse) {
	req := UpdateRequest{
		Files: infos,
	}
	rep := UpdateResponse{}
	err := worker.Call("Mirror.Update", &req, &rep)
	if err != nil {
		log.Println("Mirror.Update failure", err)
	}
}

func (me *Master) replayFileModifications(worker *rpc.Client, infos []AttrResponse) {
	entries := make(map[string]*AttrResponse)
	names := []string{}
	for i, info := range infos {
		names = append(names, info.Path)
		entries[info.Path] = &infos[i]
	}

	// Sort so we get parents before children.
	sort.SortStrings(names)
	for _, name := range names {
		info := entries[name]
		var err os.Error

		if info.FileInfo.IsDirectory() {
			if name == "" {
				name = "/"
			}
			_, err = os.Lstat(name)
			if err != nil {
				log.Println("Replay mkdir:", name)
				err = os.Mkdir(name, info.FileInfo.Mode&07777)
			}
		}
		if info.Hash != nil {
			log.Printf("Replay file content %s %x", name, info.Hash)
			c := FetchFromContentServer(
				worker, "Mirror.FileContent", info.FileInfo.Size, info.Hash)
			hash := me.cache.Save(c)
			if bytes.Compare(info.Hash, hash) != 0 {
				log.Fatal("Hash mismatch.")
			}
			err = ioutil.WriteFile(info.Path, c, info.FileInfo.Mode&07777)
			if err != nil {
				err = os.Chtimes(info.Path, info.FileInfo.Atime_ns, info.FileInfo.Mtime_ns)
			}
		}
		if info.Link != "" {
			log.Println("Replay symlink:", name)
			err = os.Symlink(info.Link, info.Path)
		}
		if info.Status == fuse.ENOENT {
			log.Println("Replay delete:", name)
			err = os.Remove(info.Path)
		}
		if !info.Status.Ok() {
			log.Fatal("Unknown status for replay", info.Status)
		}

		if err != nil {
			log.Fatal("Replay error", info.Path, err)
		}
	}
}

////////////////

// Expose functionality for the local tool to use.
type LocalMaster struct {
	master *Master
}

func (me *LocalMaster) Run(req *WorkRequest, rep *WorkReply) os.Error {
	return me.master.run(req, rep)
}
