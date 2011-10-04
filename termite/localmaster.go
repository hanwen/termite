package termite

import (
	"log"
	"net"
	"os"
	"path/filepath"
	"rpc"
	"time"
)

// Expose functionality for the local tool to use.
type LocalMaster struct {
	master   *Master
	listener net.Listener
	server   *rpc.Server
}

func localStart(m *Master, sock string) {
	me := LocalMaster{
		master: m,
	}
	me.server = rpc.NewServer()
	me.server.Register(&me)
	me.start(sock)
}

func (me *LocalMaster) Run(req *WorkRequest, rep *WorkResponse) os.Error {
	if req.RanLocally {
		log.Println("Ran command locally:", req.Argv)
		return nil
	}

	return me.master.run(req, rep)
}

func (me *LocalMaster) Shutdown(req *int, rep *int) os.Error {
	time.AfterFunc(1e8, func() {
		me.listener.Close()
	})
	return nil
}

func (me *LocalMaster) RefreshAttributeCache(input *int, output *int) os.Error {
	log.Println("Refreshing attribute cache")
	me.master.refreshAttributeCache()
	log.Println("Refresh done")
	return nil
}

func (me *LocalMaster) InspectFile(req *AttrRequest, rep *AttrResponse) os.Error {
	return me.master.fileServer.GetAttr(req, rep)
}

func (me *LocalMaster) start(sock string) {
	absSock, err := filepath.Abs(sock)
	if err != nil {
		log.Fatal("abs", err)
	}

	fi, err := os.Stat(absSock)
	if fi != nil && fi.IsSocket() {
		conn, _ := net.Dial("unix", absSock)
		if conn != nil {
			conn.Close()
			log.Fatal("socket has someone listening: ", absSock)
		}
		// TODO - should check explicitly for the relevant error message.
		log.Println("removing dead socket", absSock)
		os.Remove(absSock)
	}

	me.listener, err = net.Listen("unix", absSock)
	defer os.Remove(absSock)
	if err != nil {
		log.Fatal("startLocalServer: ", err)
	}
	err = os.Chmod(absSock, 0700)
	if err != nil {
		log.Fatal("sock chmod", err)
	}

	writableRoot := ""
	writableRoot, err = filepath.EvalSymlinks(absSock)
	if err != nil {
		log.Fatal("EvalSymlinks", err)
	}
	writableRoot = filepath.Clean(writableRoot)
	writableRoot, _ = filepath.Split(writableRoot)
	writableRoot = filepath.Clean(writableRoot)

	me.master.writableRoot = writableRoot
	me.master.CheckPrivate()
	log.Println("accepting connections on", absSock)
	for {
		conn, err := me.listener.Accept()
		if err == os.EINVAL {
			break
		}
		if err != nil {
			log.Fatal("listener.accept: ", err)
		}
		if !me.master.pending.Accept(conn) {
			go me.server.ServeConn(conn)
		}
	}
}
