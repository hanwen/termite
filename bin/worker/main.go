package main

import (
	"github.com/hanwen/termite/termite"
	"flag"
	"io/ioutil"
	"log"
	"os"
	"os/signal"
	"syscall"
)

var _ = log.Printf

func handleStop(daemon *termite.WorkerDaemon) {
	for {
		sig := <-signal.Incoming
		switch sig.(os.UnixSignal) {
		case syscall.SIGTERM, syscall.SIGQUIT, syscall.SIGINT, syscall.SIGHUP:
			log.Println("got signal: ", sig)
			var i, j int
			daemon.Shutdown(&i, &j)
		}
	}
}

func main() {
	cachedir := flag.String("cachedir", "/var/cache/termite/worker-cache", "content cache")
	tmpdir := flag.String("tmpdir", "/var/tmp",
		"where to create FUSE mounts; should be on same partition as cachedir.")
	secretFile := flag.String("secret", "secret.txt", "file containing password.")
	port := flag.Int("port", 1235, "Where to listen for work requests.")
	coordinator := flag.String("coordinator", "", "Where to register the worker.")
	jobs := flag.Int("jobs", 1, "Max number of jobs to run.")
	user := flag.String("user", "nobody", "Run as this user.")
	memcache := flag.Int("filecache", 1024, "number of <32k files to cache in memory")
	flag.Parse()

	if os.Geteuid() != 0 {
		log.Fatal("This program must run as root")
	}
	secret, err := ioutil.ReadFile(*secretFile)
	if err != nil {
		log.Fatal("ReadFile", err)
	}

	opts := termite.WorkerOptions{
		Secret:           secret,
		TempDir:          *tmpdir,
		CacheDir:         *cachedir,
		Jobs:             *jobs,
		User:             user,
		FileContentCount: *memcache,
	}

	daemon := termite.NewWorkerDaemon(&opts)
	go handleStop(daemon)
	daemon.RunWorkerServer(*port, *coordinator)
}
