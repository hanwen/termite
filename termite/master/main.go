package main

import (
	"flag"
	"fmt"
	"github.com/hanwen/go-fuse/rpcfs"
	"net"
	"rpc"
	"log"
	"os"
	"strings"
)

func SetupWorkers(addresses string, secret []byte) (out []*rpc.Client) {
	for _, addr := range strings.Split(addresses, ",", -1) {
		conn, err := rpcfs.SetupClient(addr, secret)
		if err != nil {
			log.Println("Failed setting up connection with: ", addr, err)
			continue
		}
		out = append(out, rpc.NewClient(conn))
	}
	return out
}

func StartServer(fileServer *rpcfs.FsServer, addr string, secret []byte) {
	out := make(chan net.Conn)
	go rpcfs.SetupServer(addr, secret, out)

	for {
		conn := <-out
		rpcServer := rpc.NewServer()
		err := rpcServer.Register(fileServer)
		if err != nil {
			log.Fatal("could not register file server", err)
		}
		log.Println("Server started...")
		go rpcServer.ServeConn(conn)
	}
}

func main() {
	cachedir := flag.String("cachedir", "/tmp/fsserver-cache", "content cache")
	serverAddress := flag.String("fileserver", "localhost:1234", "local file server")
	workers := flag.String("workers", "localhost:1235", "comma separated list of worker addresses")
	secretString := flag.String("secret", "secr3t", "shared password for authentication")

	flag.Parse()
	if flag.NArg() < 1 {
		log.Fatalf("usage: %s CWD COMMAND ARGS\n", os.Args[0])
	}

	fileServer := rpcfs.NewFsServer("/", *cachedir)

	secret := []byte(*secretString)

	log.Println("Starting fileserver")
	go StartServer(fileServer, *serverAddress, secret)
	log.Println("Started fileserver")
	workReq := rpcfs.WorkRequest{
	Argv: flag.Args()[1:],
	Env: os.Environ(),
	Dir: flag.Arg(0),
	FileServer: *serverAddress,
	}
	workServers := SetupWorkers(*workers, secret)

	workRep := rpcfs.WorkReply{}
	err := workServers[0].Call("WorkerDaemon.Run", &workReq, &workRep)
	if err != nil {
		log.Fatal("WorkerDaemon.Run:", err)
	} else {
		fmt.Println("REPLY:", workRep)
	}
}



