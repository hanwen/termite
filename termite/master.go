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
)


type Master struct {
	cache             *DiskFileCache
	fileServer        *FsServer
	fileServerAddress string
	secret            []byte
	workServers       []*rpc.Client
	workServerConns   []net.Conn

	masterRun    *LocalMaster
	writableRoot string

	pending *PendingConnections
}

func NewMaster(cacheDir string, workers []string, secret []byte, excluded []string) *Master {
	c := NewDiskFileCache(cacheDir)
	me := &Master{
		cache:      c,
		fileServer: NewFsServer("/", c, excluded),
		secret:     secret,
	}
	me.masterRun = &LocalMaster{me}
	me.setupWorkers(workers)
	me.secret = secret
	me.pending = NewPendingConnections()
	return me
}

func (me *Master) Start(port int, mySocket string) {
	go me.startServer(me.fileServer, port)
	me.startLocalServer(mySocket)
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

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Fatal("listener.accept", err)
		}
		me.pending.Accept(conn)
	}
}

func (me *Master) startLocalServer(sock string) {
	go me.listenLocal(sock)

	for {
		rpcServer := rpc.NewServer()
		err := rpcServer.Register(me.masterRun)
		if err != nil {
			log.Fatal("could not register server", err)
		}

		conn := me.pending.WaitConnection(RPC_CHANNEL)
		go rpcServer.ServeConn(conn)
	}
}

func (me *Master) setupWorker(addr string) net.Conn {
	conn, err := DialTypedConnection(addr, RPC_CHANNEL, me.secret)
	if err != nil {
		log.Println("Failed setting up connection with: ", addr, err)
		return nil
	}
	log.Println("done header")
	return conn
}

func (me *Master) setupWorkers(addresses []string) {
	for _, addr := range addresses {
		c := me.setupWorker(addr)
		if c != nil {
			me.workServers = append(me.workServers, rpc.NewClient(c))
			me.workServerConns = append(me.workServerConns, c)
		}
	}

	if len(me.workServerConns) == 0 {
		log.Fatal("No workers available.")
	}
}

// StartServer starts the connection listener.  Should be invoked in a coroutine.
func (me *Master) startServer(server interface{}, port int) {
	out := make(chan net.Conn)
	me.fileServerAddress = MyAddress(port)
	go SetupServer(port, me.secret, out)
	for {
		conn := <-out
		rpcServer := rpc.NewServer()
		err := rpcServer.Register(server)
		if err != nil {
			log.Fatal("could not register server", err)
		}
		log.Println("Server started...")
		go rpcServer.ServeConn(conn)
	}
}

func (me *Master) run(req *WorkRequest, rep *WorkReply) os.Error {
	idx := rand.Intn(len(me.workServers))
	worker := me.workServers[idx]

	req.FileServer = me.fileServerAddress
	req.WritableRoot = me.writableRoot

	inputConn := me.pending.WaitConnection(req.StdinId)
	destInputConn, err := DialTypedConnection(me.workServerConns[idx].RemoteAddr().String(),
		req.StdinId, me.secret)
	if err != nil {
		return err
	}

	go func() {
		HookedCopy(destInputConn, inputConn, PrintStdinSliceLen)
		destInputConn.Close()
		inputConn.Close()
	}()

	localRep := *rep
	err = worker.Call("WorkerDaemon.Run", &req, &localRep)
	if err != nil {
		return err
	}
	me.replayFileModifications(worker, localRep.Files)
	*rep = localRep
	rep.Files = nil

	go me.broadcastFiles(worker, localRep.Files)
	return err
}

func (me *Master) broadcastFiles(origin *rpc.Client, infos []AttrResponse) {
	for _, w := range me.workServers {
		if w != origin {
			go me.broadcastFilesTo(w, infos)
		}
	}
}

func (me *Master) broadcastFilesTo(worker *rpc.Client, infos []AttrResponse) {
	req := UpdateRequest{
		Files: infos,
		FileServer: me.fileServerAddress,
		WritableRoot: me.writableRoot,
	}
	rep := UpdateResponse{}
	err := worker.Call("WorkerDaemon.Update", &req, &rep)
	if err != nil {
		log.Println("WorkerDaemon.Update failure", err)
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
				worker, "WorkerDaemon.FileContent", info.FileInfo.Size, info.Hash)
			hash := me.cache.Save(c)
			if bytes.Compare(info.Hash, hash) != 0 {
				log.Fatal("Hash mismatch.")
			}
			err = ioutil.WriteFile(info.Path, c, info.FileInfo.Mode&07777)
			if err != nil {
				err = os.Chtimes(info.Path, info.FileInfo.Atime_ns,  info.FileInfo.Mtime_ns)
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

func (me *LocalMaster) Quit(ignoreIn *int, ignoreOut *int) os.Error {
	// TODO - make this actually do something.
	return nil
}
