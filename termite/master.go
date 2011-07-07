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
	rpcClient     *rpc.Client
	connection    net.Conn
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
	sync.Mutex
	sync.Cond
	mirrors       []*mirrorConnection
	master        *Master
	availableJobs int
	maxJobs       int
	workers       []string
}

func newMirrorConnections(m *Master, workers []string, maxJobs int) *mirrorConnections {
	mc := &mirrorConnections{
		master:  m,
		maxJobs: maxJobs,
		workers: workers,
	}

	mc.Cond.L = &mc.Mutex
	return mc
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

	// TODO - check if they are alive.
	if len(me.mirrors) == 0 {
		err := me.connectAll()
		if err != nil {
			return nil, err
		}
	}

	for me.availableJobs <= 0 {
		me.Cond.Wait()
	}

	l := len(me.mirrors)
	start := rand.Intn(l)
	var found *mirrorConnection
	for i := 0; i < l && found == nil; i++ {
		j := (i + start) % l
		if me.mirrors[j].availableJobs > 0 {
			found = me.mirrors[j]
		}
	}
	found.availableJobs--
	me.availableJobs--
	return found, nil
}

func (me *mirrorConnections) done(mc *mirrorConnection) {
	me.Mutex.Lock()
	defer me.Mutex.Unlock()
	me.availableJobs++
	mc.availableJobs++
	me.Cond.Signal()
}

func (me *mirrorConnections) connectAll() os.Error {
	out := make(chan *mirrorConnection, 1)
	workerJobs := 1 + me.maxJobs/len(me.workers)
	for _, addr := range me.workers {
		go func(a string) {
			conn, err := me.master.createMirror(a, workerJobs)
			if err != nil {
				log.Println("nonfatal", err)
			}
			out <- conn
		}(addr)
	}

	me.availableJobs = 0
	for _, _ = range me.workers {
		c := <-out
		if c != nil {
			me.mirrors = append(me.mirrors, c)
			me.availableJobs += c.availableJobs
		}
	}

	if len(me.mirrors) == 0 {
		return os.NewError("No workers available")
	}
	return nil
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
		go func(c net.Conn) {
			me.localRpcServer.ServeConn(conn)
			c.Close()
		}(conn)
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

func (me *Master) runOnMirror(req *WorkRequest, rep *WorkReply) (*mirrorConnection, os.Error) {
	mirror, err := me.mirrors.pick()
	if err != nil {
		return nil, err
	}
	defer me.mirrors.done(mirror)

	// Tunnel stdin.
	inputConn := me.pending.WaitConnection(req.StdinId)

	destInputConn, err := DialTypedConnection(mirror.connection.RemoteAddr().String(),
		req.StdinId, me.secret)
	if err != nil {
		return nil, err
	}
	go func() {
		HookedCopy(destInputConn, inputConn, PrintStdinSliceLen)
		destInputConn.Close()
		inputConn.Close()
	}()

	err = mirror.rpcClient.Call("Mirror.Run", &req, &rep)
	// TODO - should disconnect mirror in case of error?
	return mirror, err
}

func (me *Master) run(req *WorkRequest, rep *WorkReply) os.Error {
	localRep := *rep
	mirror, err := me.runOnMirror(req, &localRep)
	if err != nil {
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
			c, err := FetchFromContentServer(
				worker, "Mirror.FileContent", info.FileInfo.Size, info.Hash)
			if err == nil {
				hash := me.cache.Save(c)
				if bytes.Compare(info.Hash, hash) != 0 {
					log.Fatal("Hash mismatch.")
				}
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
