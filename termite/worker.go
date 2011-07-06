package termite

import (
	"fmt"
	"log"
	"os"
	"rpc"
	"sync"
	"net"
)

var _ = log.Println

type WorkReply struct {
	Exit   *os.Waitmsg
	Files  []AttrResponse
	Stderr string
	Stdout string
}


type WorkRequest struct {
	// Id of connection streaming stdin.
	StdinId      string
	Debug        bool
	FileServer   string
	WritableRoot string
	Binary       string
	Argv         []string
	Env          []string
	Dir          string
}

func (me WorkRequest) String() string {
	return fmt.Sprintf("%x %s:%s.\ncmd %s", me.StdinId, me.FileServer, me.WritableRoot,
		me.Argv)
}

type WorkerDaemon struct {
	secret       []byte
	ChrootBinary string

	// TODO - deal with closed connections.
	mirrorMapMutex sync.Mutex
	mirrorMap      map[string]*Mirror

	contentCache  *DiskFileCache
	contentServer *ContentServer

	pending *PendingConnections
}

func (me *WorkerDaemon) getMirror(rpcConn, revConn net.Conn) (*Mirror, os.Error) {
	log.Println("Mirror for", rpcConn, revConn)
	me.mirrorMapMutex.Lock()
	defer me.mirrorMapMutex.Unlock()

	key := fmt.Sprintf("%v", rpcConn.RemoteAddr())
	mirror, ok := me.mirrorMap[key]
	if ok {
		panic("huh")
	}

	mirror = &Mirror{
		fileServerConn: revConn,
		rpcConn: 	rpcConn,
		fileServer:         rpc.NewClient(revConn),
		daemon:             me,
		workingFileSystems: make(map[*WorkerFuseFs]string),
	}
	mirror.rpcFs = NewRpcFs(mirror.fileServer, me.contentCache)
	mirror.shutdownCond.L = &mirror.fuseFileSystemsMutex
	me.mirrorMap[key] = mirror
	mirror.key = key
	go mirror.serveRpc()
	return mirror, nil
}

func NewWorkerDaemon(secret []byte, cacheDir string) *WorkerDaemon {
	cache := NewDiskFileCache(cacheDir)
	w := &WorkerDaemon{
		secret:        secret,
		contentCache:  cache,
		mirrorMap:     make(map[string]*Mirror),
		contentServer: &ContentServer{Cache: cache},
		pending:       NewPendingConnections(),
	}
	return w
}

// TODO - should expose under ContentServer name?
func (me *WorkerDaemon) FileContent(req *ContentRequest, rep *ContentResponse) os.Error {
	return me.contentServer.FileContent(req, rep)
}

func trim(s string) string {
	l := 1024
	if len(s) < l {
		l = len(s)
	}
	return s[:l]
}

type CreateMirrorRequest struct {
	RpcId        string
	RevRpcId     string
	WritableRoot string
}

type CreateMirrorResponse struct {

}

func (me *WorkerDaemon) CreateMirror(req *CreateMirrorRequest, rep *CreateMirrorResponse) os.Error {
	log.Println("CreateMirror")
	rpcConn := me.pending.WaitConnection(req.RpcId)
	revConn := me.pending.WaitConnection(req.RevRpcId)
	mirror, err := me.getMirror(rpcConn, revConn)
	if err != nil {
		return err
	}
	mirror.writableRoot = req.WritableRoot
	return nil
}
func (me *WorkerDaemon) DropMirror(mirror *Mirror) {
	me.mirrorMapMutex.Lock()
	defer me.mirrorMapMutex.Unlock()
	
	log.Println("dropping mirror", mirror.key)
	me.mirrorMap[mirror.key] = nil, false
}

func (me *WorkerDaemon) RunWorkerServer(port int) {
	go me.listen(port)

	fmt.Println("RunWorkerServer")
	rpcServer := rpc.NewServer()
	rpcServer.Register(me)
	conn := me.pending.WaitConnection(RPC_CHANNEL)
	rpcServer.ServeConn(conn)
}

func (me *WorkerDaemon) listen(port int) {
	out := make(chan net.Conn)
	go SetupServer(port, me.secret, out)
	for {
		conn := <-out
		log.Println("Authenticated connection from", conn.RemoteAddr())
		me.pending.Accept(conn)
	}
}

type StatusRequest struct {

}

type StatusReply struct {
	Processes int
}

func (me *WorkerDaemon) Status(req *StatusRequest, rep *StatusReply) os.Error {
	me.mirrorMapMutex.Lock()
	defer me.mirrorMapMutex.Unlock()
	for _, mirror := range me.mirrorMap {
		mirror.Status(req, rep)
	}

	// Always return nil, so we know any errors are due to connection problems.
	return nil
}
