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
	httpPort := flag.Int("http-port", 1296, "Where to serve HTTP status.")
	chrootBinary := flag.String("chroot", "", "binary to use for chroot'ing.")
	jobs := flag.Int("jobs", 1, "Max number of jobs to run.")
	flag.Parse()
	secret, err := ioutil.ReadFile(*secretFile)
	if err != nil {
		log.Fatal("ReadFile", err)
	}

	daemon := termite.NewWorkerDaemon(secret, *cachedir, *jobs)
	daemon.ChrootBinary = *chrootBinary
	go daemon.RunWorkerServer(*port)

	daemon.ServeHTTPStatus(*httpPort)
}
