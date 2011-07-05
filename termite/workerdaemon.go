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

func (me *WorkerDaemon) newMirror(addr string, writableRoot string) (*Mirror, os.Error) {
	conn, err := SetupClient(addr, me.secret)
	if err != nil {
		return nil, err
	}

	w := &Mirror{
		fileServer:   rpc.NewClient(conn),
		daemon:       me,
		writableRoot: writableRoot,
		workingFileSystems: make(map[*WorkerFuseFs]string),
	}
	w.rpcFs = NewRpcFs(w.fileServer, me.contentCache)

	return w, nil
}

type WorkerDaemon struct {
	secret       []byte
	ChrootBinary string

	// TODO - deal with closed connections.
	masterMapMutex sync.Mutex
	masterMap      map[string]*Mirror

	contentCache   *DiskFileCache
	contentServer  *ContentServer

	pending *PendingConnections
}

func (me *WorkerDaemon) getMirror(addr string, writableRoot string) (*Mirror, os.Error) {
	me.masterMapMutex.Lock()
	defer me.masterMapMutex.Unlock()

	key := fmt.Sprintf("%s:%s", addr, writableRoot)
	mw, ok := me.masterMap[key]
	if ok {
		return mw, nil
	}

	mw, err := me.newMirror(addr, writableRoot)
	if err != nil {
		return nil, err
	}
	log.Println("Created new Mirror for", key)
	me.masterMap[key] = mw
	return mw, err
}


func NewWorkerDaemon(secret []byte, cacheDir string) *WorkerDaemon {
	cache := NewDiskFileCache(cacheDir)
	w := &WorkerDaemon{
		secret:        secret,
		contentCache:  cache,
		masterMap:     make(map[string]*Mirror),
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

func (me *WorkerDaemon) Run(req *WorkRequest, rep *WorkReply) os.Error {
	log.Println("Received", req)
	wm, err := me.getMirror(req.FileServer, req.WritableRoot)
	if err != nil {
		return err
	}

	return wm.Run(req, rep)
}

func (me *WorkerDaemon) Update(req *UpdateRequest, rep *UpdateResponse) os.Error {
	wm, err := me.getMirror(req.FileServer, req.WritableRoot)
	if err != nil { return err }
	return wm.rpcFs.Update(req, rep)
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
		log.Println("connection from", conn.RemoteAddr())
		me.pending.Accept(conn)
	}
}

type StatusRequest struct {
}

type StatusReply struct {
	Processes int
}

func (me *WorkerDaemon) Status(req *StatusRequest, rep *StatusReply) os.Error {
	me.masterMapMutex.Lock()
	defer me.masterMapMutex.Unlock()
	for _, mirror := range me.masterMap {
		mirror.Status(req, rep)
	}

	// Always return nil, so we know any errors are due to connection problems.
	return nil
}
