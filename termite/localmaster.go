package termite

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"

	"github.com/hanwen/termite/attr"
)

// Expose functionality for the local tool to use.
type LocalMaster struct {
	master   *Master
	listener connListener
	server   *rpc.Server
}

func localStart(m *Master, sock string) {
	l := LocalMaster{
		master: m,
	}
	l.server = rpc.NewServer()
	l.server.Register(&l)
	l.start(sock)
}

func (m *LocalMaster) Run(req *WorkRequest, rep *WorkResponse) error {
	if req.RanLocally {
		log.Println("Ran command locally:", req.Argv)
		return nil
	}
	if len(req.Binary) == 0 || req.Binary[0] != '/' {
		return fmt.Errorf("Path to binary is not absolute: %q", req.Binary)
	}
	if req.StdinId != "" {
		req.StdinConn = m.listener.Pending().accept(req.StdinId)
	}
	return m.master.run(req, rep)
}

func (m *LocalMaster) Shutdown(req *int, rep *int) error {
	m.master.quit <- 1
	return nil
}

func (m *LocalMaster) RefreshAttributeCache(input *int, output *int) error {
	log.Println("Refreshing attribute cache")
	m.master.refreshAttributeCache()
	m.master.setAnalysisDir()
	log.Println("Refresh done")
	return nil
}

func (m *LocalMaster) InspectFile(req *attr.AttrRequest, rep *attr.AttrResponse) error {
	a := m.master.attributes.GetDir(req.Name)
	rep.Attrs = append(rep.Attrs, a)
	return nil
}

func (m *LocalMaster) start(sock string) {
	l, err := net.Listen("unix", sock)
	if err != nil {
		log.Fatal("startLocalServer: ", err)
	}
	defer os.Remove(sock)

	err = os.Chmod(sock, 0700)
	if err != nil {
		log.Fatal("sock chmod", err)
	}

	log.Println("accepting connections on", sock)
	m.listener = newTCPListener(l, nil)
	for c := range m.listener.Pending().rpcChan() {
		go m.server.ServeConn(c)
	}
}
