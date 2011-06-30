package main

import (
	"flag"
	"github.com/hanwen/go-fuse/rpcfs"
	"net"
	"rpc"
	"log"
	"http"
	"os"
	"fmt"
)

func main() {
	port := flag.Int("port", 1234, "Port to listen to.")
	cachedir := flag.String("cachedir", "/tmp/fsserver-cache", "content cache")
	flag.Parse()
	if flag.NArg() < 1 {
		log.Fatal(os.Stderr, "usage: %s EXPORTED-ROOT\n", os.Args[0])
	}
	
	files := rpcfs.NewFsServer(flag.Arg(0), *cachedir)

	rpc.Register(files)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", fmt.Sprintf("localhost:%d", *port))
	if e != nil {
		log.Fatal("listen error:", e)
	}
	http.Serve(l, nil)
}
	

