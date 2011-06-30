package main

import (
	"github.com/hanwen/go-fuse/fuse"
	"github.com/hanwen/go-fuse/rpcfs"
	"fmt"
	"flag"
	"log"
	"os"
	"rpc"
)

var _ = log.Printf

func main() {
	flag.Parse()
	if flag.NArg() < 2 {
		fmt.Fprintf(os.Stderr, "usage: %s MOUNTPOINT SERVER\n", os.Args[0])
		os.Exit(2)
	}

	client, err := rpc.DialHTTP("tcp", flag.Arg(1))
	if err != nil {
		log.Fatal("dialing:", err)
	}

	var fs fuse.FileSystem
	fs = rpcfs.NewRpcFs(client)
	conn := fuse.NewFileSystemConnector(fs, nil)
	state := fuse.NewMountState(conn)
	state.Mount(flag.Arg(0), &fuse.MountOptions{AllowOther: true})
	if err != nil {
		fmt.Printf("Mount fail: %v\n", err)
		os.Exit(1)
	}

	state.Debug = true
	state.Loop(true)
}

