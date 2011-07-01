package main

import (
	"flag"
	"github.com/hanwen/go-fuse/rpcfs"
	"io/ioutil"
	"net"
	"rpc"
	"log"
	"os"
)

func main() {
	cachedir := flag.String("cachedir", "/tmp/fsserver-cache", "content cache")
	port := flag.Int("port", 1234, "file server")
	secretFile := flag.String("secret", "/tmp/secret.txt", "file containing password.")

	flag.Parse()
	if flag.NArg() < 1 {
		log.Fatalf("usage: %s EXPORTED-ROOT\n", os.Args[0])
	}
	secret, err := ioutil.ReadFile(*secretFile)
	if err != nil {
		log.Fatal("ReadFile", err)
	}
	cache := rpcfs.NewDiskFileCache(*cachedir)
	fileServer := rpcfs.NewFsServer(flag.Arg(0), cache, []string{"/proc"})

	out := make(chan net.Conn)
	go rpcfs.SetupServer(*port, secret, out)

	conn := <-out
	rpcServer := rpc.NewServer()
	err = rpcServer.Register(fileServer)
	if err != nil {
		log.Fatal("could not register file server", err)
	}
	log.Println("Server started...")
	rpcServer.ServeConn(conn)
}


