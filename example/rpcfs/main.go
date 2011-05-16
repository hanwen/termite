package main

import (
	"github.com/hanwen/go-fuse/fuse"
	"github.com/hanwen/go-fuse/rpcfs"
	"fmt"
	"flag"
	"log"
	"os"
)

var _ = log.Printf

func main() {
	flag.Parse()
	if flag.NArg() < 2 {
		fmt.Fprintf(os.Stderr, "usage: %s MOUNTPOINT SERVER\n", os.Args[0])
		os.Exit(2)
	}

	var fs fuse.FileSystem
	fs = rpcfs.NewRpcFs(flag.Arg(1))
	state, _, err := fuse.MountFileSystem(flag.Arg(0), fs, nil)
	if err != nil {
		fmt.Printf("Mount fail: %v\n", err)
		os.Exit(1)
	}

	state.Debug = true
	state.Loop(true)
}

