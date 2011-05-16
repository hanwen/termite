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
	
	if flag.NArg() < 1 {
		fmt.Fprintf(os.Stderr, "usage: %s EXPORTED-ROOT\n", os.Args[0])
		os.Exit(2)
	}
	flag.Parse()
	files := &rpcfs.FsServer{
	Root: 	flag.Arg(0),
	}

	rpc.Register(files)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", fmt.Sprintf("localhost:%d", *port))
	if e != nil {
		log.Fatal("listen error:", e)
	}
	http.Serve(l, nil)
}
	

