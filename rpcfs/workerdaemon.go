package rpcfs

import (
	"log"
	"os"
	"rpc"
	"sync"
)

var _ = log.Println

type FileInfo struct {
	Delete      bool
	Path        string
	LinkContent string
	os.FileInfo
	Hash []byte
}

type WorkReply struct {
	Exit   *os.Waitmsg
	Files  []FileInfo
	Stderr string
	Stdout string
}

type WorkRequest struct {
	FileServer string
	Binary     string
	Argv       []string
	Env        []string
	Dir        string
}

type WorkerDaemon struct {
	secret             []byte
	fileServerMapMutex sync.Mutex
	ChrootBinary       string

	// TODO - deal with closed connections.
	fileServerMap map[string]*rpc.Client
	contentCache  *DiskFileCache
	contentServer *ContentServer
}

func (me *WorkerDaemon) getFileServer(addr string) (*rpc.Client, os.Error) {
	me.fileServerMapMutex.Lock()
	defer me.fileServerMapMutex.Unlock()

	client, ok := me.fileServerMap[addr]
	if ok {
		return client, nil
	}

	conn, err := SetupClient(addr, me.secret)
	if err != nil {
		return nil, err
	}

	client = rpc.NewClient(conn)
	return client, nil
}

func NewWorkerDaemon(secret []byte, cacheDir string) *WorkerDaemon {
	cache := NewDiskFileCache(cacheDir)
	w := &WorkerDaemon{
		secret:        secret,
		contentCache:  cache,
		fileServerMap: make(map[string]*rpc.Client),
		contentServer: &ContentServer{Cache: cache},
	}
	return w
}

// TODO - should expose under ContentServer name?
func (me *WorkerDaemon) FileContent(req *ContentRequest, rep *ContentResponse) os.Error {
	return me.contentServer.FileContent(req, rep)
}

func (me *WorkerDaemon) Run(req *WorkRequest, rep *WorkReply) os.Error {
	task, err := me.newWorkerTask(req, rep)
	if err != nil {
		return err
	}

	err = task.Run()
	if err != nil {
		log.Println("Error", err)
		return err
	}


	summary := rep
	// Trim output.
	summary.Stdout = summary.Stdout[:1024]
	summary.Stderr = summary.Stderr[:1024]

	log.Println("sending back", summary)
	return nil
}
