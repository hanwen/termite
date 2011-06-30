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
	flag.Parse()
	if flag.NArg() < 3 {
		fmt.Fprintf(os.Stderr, "usage: %s SERVER CWD COMMAND ARGS\n", os.Args[0])
		os.Exit(2)
	}

	client, err := rpc.DialHTTP("tcp", flag.Arg(0))
	if err != nil {
		log.Fatal("dialing:", err)
	}

	t := rpcfs.Task{
	Argv: flag.Args()[2:],
	Env: os.Environ(),
	Dir: flag.Arg(1),
	}

	log.Println("task...")
	workertask, err := rpcfs.NewWorkerTask(client, &t)
	if err != nil {
		log.Fatal(err)
	}
	workertask.Run()

	log.Println("done...")
}

