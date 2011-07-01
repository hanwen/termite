package main

import (
	"github.com/hanwen/go-fuse/rpcfs"
	"flag"
	"io/ioutil"
	"log"
	"net"
	"rpc"
)

var _ = log.Printf

func main() {
	cachedir := flag.String("cachedir", "/tmp/worker-cache", "content cache")
	secretFile := flag.String("secret", "/tmp/secret.txt", "file containing password.")
	serverAddress := flag.String("address", "localhost:1235", "Where to listen for work requests.")
	chrootBinary := flag.String("chroot", "", "binary to use for chroot'ing.")
	flag.Parse()
	secret, err := ioutil.ReadFile(*secretFile)
	if err != nil {
		log.Fatal("ReadFile", err)
	}

	daemon := rpcfs.NewWorkerDaemon(secret, *cachedir)
	daemon.ChrootBinary = *chrootBinary

	out := make(chan net.Conn)
	go rpcfs.SetupServer(*serverAddress, secret, out)
	log.Println("Listening to", *serverAddress)
	for {
		conn := <-out
		log.Println("Opening RPC channel from", conn.RemoteAddr())
		rpcServer := rpc.NewServer()
		rpcServer.Register(daemon)
		go rpcServer.ServeConn(conn)
	}
}
