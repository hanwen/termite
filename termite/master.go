package termite

import (
	"bytes"
	"io/ioutil"
	"log"
	"net"
	"os"
	"path/filepath"
	"github.com/hanwen/go-fuse/fuse"
	"rpc"
	"sort"
	"sync"
)

type mirrorConnection struct {
	workerAddr    string	// key in map.
	rpcClient     *rpc.Client
	connection    net.Conn

	// Protected by mirrorConnections.Mutex.
	maxJobs       int
	availableJobs int
}

func (me *mirrorConnection) sendFiles(infos []AttrResponse) {
	req := UpdateRequest{
		Files: infos,
	}
	rep := UpdateResponse{}
	err := me.rpcClient.Call("Mirror.Update", &req, &rep)
	if err != nil {
		log.Println("Mirror.Update failure", err)
	}
}

type mirrorConnections struct {
	master        *Master
	workers       []string

	// Condition for mutex below.
	sync.Cond

	// Protects all of the below.
	sync.Mutex
	mirrors       map[string]*mirrorConnection
	wantedMaxJobs int
}

func newMirrorConnections(m *Master, workers []string, maxJobs int) *mirrorConnections {
	mc := &mirrorConnections{
		master:  m,
		wantedMaxJobs: maxJobs,
		workers: workers,
		mirrors: make(map[string]*mirrorConnection),
	}

	mc.Cond.L = &mc.Mutex
	return mc
}

func (me *mirrorConnections) availableJobs() int {
	a := 0
	for _, mc := range me.mirrors {
		a += mc.availableJobs
	}
	return a
}

func (me *mirrorConnections) maxJobs() int {
	a := 0
	for _, mc := range me.mirrors {
		a += mc.maxJobs
	}
	return a
}

func (me *mirrorConnections) broadcastFiles(origin *mirrorConnection, infos []AttrResponse) {
	for _, w := range me.mirrors {
		if origin != w {
			go w.sendFiles(infos)
		}
	}
}

// Gets a mirrorConnection to run on.  Will block if none available
func (me *mirrorConnections) pick() (*mirrorConnection, os.Error) {
	me.Mutex.Lock()
	defer me.Mutex.Unlock()

	if me.availableJobs() <= 0 {
		me.tryConnect()
	}

	if me.maxJobs() == 0 {
		// Didn't connect to anything.  Should
		// probably direct the wrapper to compile
		// locally.
		return nil, os.NewError("No workers found at all.")
	}

	for me.availableJobs() == 0 {
		me.Cond.Wait()
	}

	var found *mirrorConnection
	for _, v := range me.mirrors {
		if v.availableJobs > 0 {
			found = v
		}
	}
	found.availableJobs--
	return found, nil
}

func (me *mirrorConnections) drop(mc *mirrorConnection, err os.Error) {
	me.Mutex.Lock()
	defer me.Mutex.Unlock()

	// TODO - should blacklist the address.
	log.Printf("Dropping mirror %s. Reason: %s", mc.workerAddr, err)
	mc.connection.Close()
	me.mirrors[mc.workerAddr] = nil, false
}

func (me *mirrorConnections) jobDone(mc *mirrorConnection) {
	me.Mutex.Lock()
	defer me.Mutex.Unlock()

	mc.availableJobs++
	me.Cond.Signal()
}

// Tries to connect to one extra worker.
func (me *mirrorConnections) tryConnect() {
	// We want to max out capacity of each worker, as that helps
	// with cache hit rates on the worker.
	wanted := me.wantedMaxJobs - me.maxJobs()
	if wanted <= 0 {
		return
	}

	for _, addr := range me.workers {
		_, ok := me.mirrors[addr]
		if ok { continue }
		mc, err := me.master.createMirror(addr, wanted)
		if err != nil {
			log.Println("nonfatal", err)
			continue
		}
		mc.workerAddr = addr
		me.mirrors[addr] = mc
	}
}


type Master struct {
	cache         *DiskFileCache
	fileServer    *FsServer
	fileServerRpc *rpc.Server
	secret        []byte

	mirrors        *mirrorConnections
	localRpcServer *rpc.Server
	localServer    *LocalMaster
	writableRoot   string

	pending *PendingConnections
}

func NewMaster(cache *DiskFileCache, workers []string, secret []byte, excluded []string, maxJobs int) *Master {
	me := &Master{
		cache:      cache,
		fileServer: NewFsServer("/", cache, excluded),
		secret:     secret,
	}
	me.mirrors = newMirrorConnections(me, workers, maxJobs)
	me.localServer = &LocalMaster{me}
	me.secret = secret
	me.pending = NewPendingConnections()
	me.fileServerRpc = rpc.NewServer()
	me.fileServerRpc.Register(me.fileServer)
	me.localRpcServer = rpc.NewServer()
	me.localRpcServer.Register(me.localServer)
	return me
}

func (me *Master) Start(sock string) {
	absSock, err := filepath.Abs(sock)
	if err != nil {
		log.Fatal("abs", err)
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

	log.Println("Accepting connections on", absSock)
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

	err = mirror.rpcClient.Call("Mirror.Run", &req, &rep)
	return err
}

func (me *Master) run(req *WorkRequest, rep *WorkReply) os.Error {
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

	me.replayFileModifications(mirror.rpcClient, localRep.Files)
	*rep = localRep
	rep.Files = nil

	go me.mirrors.broadcastFiles(mirror, localRep.Files)
	return err
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
			// TODO - stream directly from network connection to file.
			c, err := FetchFromContentServer(
				worker, "Mirror.FileContent", info.FileInfo.Size, info.Hash)
			if err == nil {
				hash := me.cache.Save(c)
				if bytes.Compare(info.Hash, hash) != 0 {
					log.Fatal("Hash mismatch.")
				}
				// TODO - should allow a mode to set a hard or symbolic link.
				err = ioutil.WriteFile(info.Path, c, info.FileInfo.Mode&07777)
			}
			if err == nil {
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
