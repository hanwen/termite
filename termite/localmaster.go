package termite

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"syscall"

	"github.com/hanwen/termite/attr"
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

func (me *LocalMaster) Run(req *WorkRequest, rep *WorkResponse) error {
	if req.RanLocally {
		log.Println("Ran command locally:", req.Argv)
		return nil
	}
	if len(req.Binary) == 0 || req.Binary[0] != '/' {
		return fmt.Errorf("Path to binary is not absolute: %q", req.Binary)
	}

	return me.master.run(req, rep)
}

func (me *LocalMaster) Shutdown(req *int, rep *int) error {
	me.master.quit <- 1
	return nil
}

func (me *LocalMaster) RefreshAttributeCache(input *int, output *int) error {
	log.Println("Refreshing attribute cache")
	me.master.refreshAttributeCache()
	log.Println("Refresh done")
	return nil
}

func (me *LocalMaster) InspectFile(req *attr.AttrRequest, rep *attr.AttrResponse) error {
	return me.master.fileServer.GetAttr(req, rep)
}

func (me *LocalMaster) start(sock string) {
	l, err := net.Listen("unix", sock)
	if err != nil {
		log.Fatal("startLocalServer: ", err)
	}
	me.listener = l
	defer os.Remove(sock)

	err = os.Chmod(sock, 0700)
	if err != nil {
		log.Fatal("sock chmod", err)
	}

	log.Println("accepting connections on", sock)
	for {
		conn, err := me.listener.Accept()
		if err == syscall.EINVAL {
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
