package main

import (
	"github.com/hanwen/go-fuse/termite"
	"flag"
	"io/ioutil"
	"log"
)

var _ = log.Printf

func main() {
	cachedir := flag.String("cachedir", "/tmp/worker-cache", "content cache")
	secretFile := flag.String("secret", "/tmp/secret.txt", "file containing password.")
	port := flag.Int("port", 1235, "Where to listen for work requests.")
	chrootBinary := flag.String("chroot", "", "binary to use for chroot'ing.")
	flag.Parse()
	secret, err := ioutil.ReadFile(*secretFile)
	if err != nil {
		log.Fatal("ReadFile", err)
	}

	daemon := termite.NewWorkerDaemon(secret, *cachedir)
	daemon.ChrootBinary = *chrootBinary
	daemon.RunWorkerServer(*port)
}
