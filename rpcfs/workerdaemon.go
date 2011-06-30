package rpcfs

import (
	"log"
	"os"
	"rpc"
	"sync"
	)

var _ = log.Println

type FileInfo struct {
	Delete bool
	os.FileInfo
	Hash []byte
}

type WorkReply struct {
	*os.Waitmsg
	Files  []*FileInfo
	Stderr []byte
	Stdout []byte
}

type WorkRequest struct {
	FileServer string
	Argv []string
	Env []string
	Dir string
}

type WorkerDaemon struct {
	secret []byte
	fileServerMapMutex 		sync.Mutex
	ChrootBinary string

	// TODO - deal with closed connections.
	fileServerMap 	map[string]*rpc.Client
	cacheDir string
}

func (me *WorkerDaemon) GetFileServer(addr string) (*rpc.Client, os.Error) {
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

func NewWorkerDaemon(secret []byte, cacheDir string) (*WorkerDaemon) {
	w := &WorkerDaemon{
	secret: secret,
	cacheDir: cacheDir,
	fileServerMap: make(map[string]*rpc.Client),
	}
	return w
}

func (me *WorkerDaemon) Run(req *WorkRequest, rep *WorkReply) os.Error {
	task, err := me.newWorkerTask(req, rep)
	if err != nil {
		return err
	}

	err = task.Run()
	if err != nil {
		return err
	}

	return nil
}

