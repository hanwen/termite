package main

import (
	"flag"
	"log"
	"github.com/hanwen/go-fuse/termite"
	"io/ioutil"
	"strings"
)

func main() {
	cachedir := flag.String("cachedir", "/tmp/fsserver-cache", "content cache")
	workers := flag.String("workers", "localhost:1235", "comma separated list of worker addresses")
	socket := flag.String("socket", ".termite-socket", "socket to listen for commands")
	exclude := flag.String("exclude", "/sys,/proc,/dev,/selinux", "prefixes to not export.")
	secretFile := flag.String("secret", "/tmp/secret.txt", "file containing password.")
	jobs := flag.Int("jobs", 1, "number of jobs to run")

	flag.Parse()
	secret, err := ioutil.ReadFile(*secretFile)
	if err != nil {
		log.Fatal("ReadFile", err)
	}

	workerList := strings.Split(*workers, ",", -1)
	excludeList := strings.Split(*exclude, ",", -1)
	c := termite.NewDiskFileCache(*cachedir)
	master := termite.NewMaster(
		c, workerList, secret, excludeList, *jobs)
	master.Start(*socket)
}
