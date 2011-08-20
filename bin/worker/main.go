package main

import (
	"github.com/hanwen/termite/termite"
	"flag"
	"io/ioutil"
	"log"
	"os"
)

var _ = log.Printf

func main() {
	cachedir := flag.String("cachedir", "/var/cache/termite/worker-cache", "content cache")
	tmpdir := flag.String("tmpdir", "/var/tmp",
		"where to create FUSE mounts; should be on same partition as cachedir.")
	secretFile := flag.String("secret", "secret.txt", "file containing password.")
	port := flag.Int("port", 1235, "Where to listen for work requests.")
	coordinator := flag.String("coordinator", "", "Where to register the worker.")
	jobs := flag.Int("jobs", 1, "Max number of jobs to run.")
	flag.Parse()

	if os.Geteuid() != 0 {
		log.Fatal("This program must run as root")
	}
	secret, err := ioutil.ReadFile(*secretFile)
	if err != nil {
		log.Fatal("ReadFile", err)
	}

	daemon := termite.NewWorkerDaemon(secret, *tmpdir, *cachedir, *jobs)
	daemon.RunWorkerServer(*port, *coordinator)
}
