package main

import (
	"flag"
	"github.com/hanwen/go-fuse/rpcfs"
	"net"
	"rpc"
	"log"
	"os"
)

func main() {
	cachedir := flag.String("cachedir", "/tmp/fsserver-cache", "content cache")
	server := flag.String("server", "localhost:1234", "file server")
	secret := flag.String("secret", "secr3t", "shared password for authentication")

	flag.Parse()
	if flag.NArg() < 1 {
		log.Fatalf("usage: %s EXPORTED-ROOT\n", os.Args[0])
	}

	fileServer := rpcfs.NewFsServer(flag.Arg(0), *cachedir)

	out := make(chan net.Conn)
	go rpcfs.SetupServer(*server, []byte(*secret), out)

	conn := <-out
	rpcServer := rpc.NewServer()
	err := rpcServer.Register(fileServer)
	if err != nil {
		log.Fatal("could not register file server", err)
	}
	log.Println("Server started...")
	rpcServer.ServeConn(conn)
}


