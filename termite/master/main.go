package main

import (
	"flag"
	"fmt"
	"github.com/hanwen/go-fuse/rpcfs"
	"log"
	"os"
	"strings"
)


func main() {
	cachedir := flag.String("cachedir", "/tmp/fsserver-cache", "content cache")
	serverAddress := flag.String("fileserver", "localhost:1234", "local file server")
	workers := flag.String("workers", "localhost:1235", "comma separated list of worker addresses")
	secretString := flag.String("secret", "secr3t", "shared password for authentication")

	flag.Parse()
	if flag.NArg() < 1 {
		log.Fatalf("usage: %s CWD COMMAND ARGS\n", os.Args[0])
	}

	workerList := strings.Split(*workers, ",", -1)
	master := rpcfs.NewMaster(
		*cachedir, *serverAddress, workerList, []byte(*secretString))

	workReq := rpcfs.WorkRequest{
	Argv: flag.Args()[1:],
	Env: os.Environ(),
	Dir: flag.Arg(0),
	FileServer: *serverAddress,
	}
	workRep := &rpcfs.WorkReply{}

	err := master.Run(&workReq, workRep)
	if err != nil {
		log.Fatal("WorkerDaemon.Run:", err)
	} else {
		fmt.Println("REPLY:", workRep)
	}
}



