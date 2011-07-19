package main

import (
	"github.com/hanwen/termite/termite"
	"flag"
	"io/ioutil"
	"log"
)

var _ = log.Printf

func main() {
	cachedir := flag.String("cachedir", "/var/cache/termite/worker-cache", "content cache")
	tmpdir := flag.String("tmpdir", "/var/tmp",
		"where to create FUSE mounts; should be on same partition as cachedir.")
	secretFile := flag.String("secret", "secret.txt", "file containing password.")
	port := flag.Int("port", 1235, "Where to listen for work requests.")
	httpPort := flag.Int("http-port", 1236, "Where to serve HTTP status.")
	coordinator := flag.String("coordinator", "", "Where to register the worker.")
	chrootBinary := flag.String("chroot", "", "binary to use for chroot'ing.")
	jobs := flag.Int("jobs", 1, "Max number of jobs to run.")
	flag.Parse()
	secret, err := ioutil.ReadFile(*secretFile)
	if err != nil {
		log.Fatal("ReadFile", err)
	}

	daemon := termite.NewWorkerDaemon(secret, *tmpdir, *cachedir, *jobs)
	daemon.ChrootBinary = *chrootBinary
	go daemon.ServeHTTPStatus(*httpPort)
	daemon.RunWorkerServer(*port, *coordinator)
}
