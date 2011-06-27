package main

import (
	"github.com/hanwen/go-fuse/rpcfs"
	"fmt"
	"flag"
	"log"
	"os"
	"rpc"
)

var _ = log.Printf

func main() {
	cachedir := flag.String("cachedir", "/tmp/cache", "content cache")
	server := flag.String("server", "localhost:1234", "file server")
	flag.Parse()
	if flag.NArg() < 3 {
		fmt.Fprintf(os.Stderr, "usage: %s CWD COMMAND ARGS\n", os.Args[0])
		os.Exit(2)
	}

	client, err := rpc.DialHTTP("tcp", *server)
	if err != nil {
		log.Fatal("dialing:", err)
	}

	t := rpcfs.Task{
	Argv: flag.Args()[1:],
	Env: os.Environ(),
	Dir: flag.Arg(0),
	}

	log.Println("task...")
	workertask, err := rpcfs.NewWorkerTask(client, &t, *cachedir)
	if err != nil {
		log.Fatal(err)
	}
	workertask.Run()

	log.Println("Done! Results in", workertask.RWDir())
}

