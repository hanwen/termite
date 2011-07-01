package main

import (
	"github.com/hanwen/go-fuse/fuse"
	"github.com/hanwen/go-fuse/termite"
	"fmt"
	"flag"
	"log"
	"os"
	"rpc"
	"io/ioutil"
)

func main() {
	cachedir := flag.String("cachedir", "/tmp/termite-cache", "content cache")
	server := flag.String("server", "localhost:1234", "file server")
	secretFile := flag.String("secret", "/tmp/secret.txt", "file containing password.")

	flag.Parse()
	if flag.NArg() < 1 {
		fmt.Fprintf(os.Stderr, "usage: %s MOUNTPOINT\n", os.Args[0])
		os.Exit(2)
	}
	secret, err := ioutil.ReadFile(*secretFile)
	if err != nil {
		log.Fatal("ReadFile", err)
	}

	rpcConn, err := termite.SetupClient(*server, secret)
	if err != nil {
		log.Fatal("dialing:", err)
	}

	var fs fuse.FileSystem
	cache := termite.NewDiskFileCache(*cachedir)
	fs = termite.NewRpcFs(rpc.NewClient(rpcConn), cache)
	conn := fuse.NewFileSystemConnector(fs, nil)
	state := fuse.NewMountState(conn)
	opts := fuse.MountOptions{}
	if os.Geteuid() == 0 {
		opts.AllowOther = true
	}

	state.Mount(flag.Arg(0), &opts)
	if err != nil {
		fmt.Printf("Mount fail: %v\n", err)
		os.Exit(1)
	}

	state.Debug = true
	state.Loop(false)
}

