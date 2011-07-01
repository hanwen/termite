package main

import (
	"flag"
	"github.com/hanwen/go-fuse/rpcfs"
	"strings"
)

func main() {
	cachedir := flag.String("cachedir", "/tmp/fsserver-cache", "content cache")
	serverAddress := flag.String("fileserver", "localhost:1234", "local file server")
	workers := flag.String("workers", "localhost:1235", "comma separated list of worker addresses")
	secretString := flag.String("secret", "secr3t", "shared password for authentication")
	socket := flag.String("socket", "/tmp/termite-socket", "socket to listen for commands")
	exclude := flag.String("exclude", "/proc", "prefixes to not export.")

	flag.Parse()
	workerList := strings.Split(*workers, ",", -1)
	excludeList := strings.Split(*exclude, ",", -1)
	master := rpcfs.NewMaster(
		*cachedir, workerList, []byte(*secretString), excludeList)
	master.Start(*serverAddress, *socket)
}



