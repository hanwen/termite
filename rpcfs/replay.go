package rpcfs

import (
	"bytes"
	"io/ioutil"
	"log"
	"net"
	"os"
	"rpc"
	"sort"
)


type Master struct {
	cache      *DiskFileCache
	fileServer *FsServer
	secret	    []byte
	workServers []*rpc.Client
}

func NewMaster(cacheDir string, myAddress string, workers []string, secret []byte) *Master {
	c := NewDiskFileCache(cacheDir)
	me := &Master{
	cache: c,
	fileServer: NewFsServer("/", c),
	secret: secret,
	}

	me.SetupWorkers(workers)
	me.secret = secret
	go me.StartServer(me.fileServer, myAddress)
	return me
}

func (me *Master) SetupWorkers(addresses []string) {
	var out []*rpc.Client
	for _, addr := range addresses {
		conn, err := SetupClient(addr, me.secret)
		if err != nil {
			log.Println("Failed setting up connection with: ", addr, err)
			continue
		}
		out = append(out, rpc.NewClient(conn))
	}
	if len(out) == 0 {
		log.Fatal("No workers available.")
	}
	me.workServers = out
}

// StartServer starts the connection listener.  Should be invoked in a coroutine.
func (me *Master) StartServer(fileServer *FsServer, addr string) {
	out := make(chan net.Conn)
	go SetupServer(addr, me.secret, out)

	for {
		conn := <-out
		rpcServer := rpc.NewServer()
		err := rpcServer.Register(fileServer)
		if err != nil {
			log.Fatal("could not register file server", err)
		}
		log.Println("Server started...")
		go rpcServer.ServeConn(conn)
	}
}

func (me *Master) Run(req *WorkRequest, rep *WorkReply) os.Error {
	// TODO: random pick.
	worker := me.workServers[0]

	err := worker.Call("WorkerDaemon.Run", &req, &rep)
	if err != nil {
		return err
	}
	me.replayFileModifications(worker, rep.Files)
	rep.Files = nil
	return err
}

func (me *Master) replayFileModifications(worker *rpc.Client, infos []FileInfo) {
	entries := make(map[string]*FileInfo)
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
				err = os.Mkdir(name, info.FileInfo.Mode & 07777)
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
			err = ioutil.WriteFile(info.Path, c, info.FileInfo.Mode & 07777)
		}
		if info.LinkContent != "" {
			log.Println("Replay symlink:", name)
			err = os.Symlink(info.LinkContent, info.Path)
		}
		if info.Delete {
			log.Println("Replay delete:", name)
			err = os.Remove(info.Path)
		}
		if err != nil {
			log.Fatal("Replay error", info.Path, err)
		}
	}
}
