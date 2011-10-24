package main

import (
	"flag"
	"github.com/hanwen/termite/termite"
	"io/ioutil"
	"log"
	"net"
	"os"
	"path/filepath"
	"strings"
)

func main() {
	home := os.Getenv("HOME")
	cachedir := flag.String("cachedir",
		filepath.Join(home, ".cache", "termite-master"), "content cache")
	workers := flag.String("workers", "", "comma separated list of worker addresses")
	coordinator := flag.String("coordinator", "localhost:1233",
		"address of coordinator. Overrides -workers")
	socket := flag.String("socket", ".termite-socket", "socket to listen for commands")
	exclude := flag.String("exclude",
		"usr/lib/locale/locale-archive,sys,proc,dev,selinux,cgroup", "prefixes to not export.")
	secretFile := flag.String("secret", "secret.txt", "file containing password.")
	srcRoot := flag.String("sourcedir", "", "root of corresponding source directory")
	jobs := flag.Int("jobs", 1, "number of jobs to run")
	port := flag.Int("port", 1237, "http status port")
	houseHoldPeriod := flag.Float64("time.household", 60.0, "how often to do house hold tasks.")
	keepAlive := flag.Float64("time.keepalive", 60.0, "for how long to keep workers reserved.")
	memcache := flag.Int("filecache", 1024, "number of <32k files to cache in memory")
	paranoia := flag.Bool("paranoia", false, "Check attribute cache.")

	flag.Parse()
	termite.Paranoia = *paranoia

	log.SetPrefix("M")

	secret, err := ioutil.ReadFile(*secretFile)
	if err != nil {
		log.Fatal("ReadFile", err)
	}

	workerList := strings.Split(*workers, ",")
	excludeList := strings.Split(*exclude, ",")
	c := termite.NewContentCache(*cachedir)
	c.SetMemoryCacheSize(*memcache)

	root, sock := absSocket(*socket)
	opts := termite.MasterOptions{
		Secret: secret,
		MaxJobs: *jobs,
		Excludes: excludeList,
		Workers: workerList,
		Coordinator: *coordinator,
		SrcRoot: *srcRoot,
		WritableRoot: root,
	}
	master := termite.NewMaster(c, &opts)
	master.SetKeepAlive(*keepAlive, *houseHoldPeriod)

	log.Println(termite.Version())

	go master.ServeHTTP(*port)
	master.Start(sock)
}

func absSocket(sock string) (root, absSock string) {
	absSock, err := filepath.Abs(sock)
	if err != nil {
		log.Fatal("abs", err)
	}

	fi, err := os.Stat(absSock)
	if fi != nil && fi.IsSocket() {
		conn, _ := net.Dial("unix", absSock)
		if conn != nil {
			conn.Close()
			log.Fatal("socket has someone listening: ", absSock)
		}
		// TODO - should check explicitly for the relevant error message.
		log.Println("removing dead socket", absSock)
		os.Remove(absSock)
	}

	root, _ = termite.SplitPath(absSock)
	root, err = filepath.EvalSymlinks(root)
	if err != nil {
		log.Fatal("EvalSymlinks", err)
	}
	root = filepath.Clean(root)
	return root, absSock
}
