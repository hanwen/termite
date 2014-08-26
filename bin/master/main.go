package main

import (
	"flag"
	"io/ioutil"
	"log"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/hanwen/termite/cba"
	"github.com/hanwen/termite/termite"
)

func main() {
	home := os.Getenv("HOME")
	cachedir := flag.String("cachedir", filepath.Join(home, ".cache", "termite-master"), "content cache")
	coordinator := flag.String("coordinator", "localhost:1230", "address of coordinator. Overrides -workers")
	exclude := flag.String("exclude", "usr/lib/locale/locale-archive,sys,proc,dev,selinux,cgroup", "prefixes to not export.")
	fetchAll := flag.Bool("fetch-all", true, "Fetch all files on startup.")
	houseHoldPeriod := flag.Float64("time.household", 60.0, "how often to do house hold tasks.")
	jobs := flag.Int("jobs", 1, "number of jobs to run")
	keepAlive := flag.Float64("time.keepalive", 60.0, "for how long to keep workers reserved.")
	logfile := flag.String("logfile", "", "where to send log output.")
	paranoia := flag.Bool("paranoia", false, "Check attribute cache.")
	port := flag.Int("port", 1231, "http status port")
	retry := flag.Int("retry", 3, "how often to retry faulty jobs")
	secretFile := flag.String("secret", "secret.txt", "file containing password or SSH identity.")
	socket := flag.String("socket", ".termite-socket", "socket to listen for commands")
	srcRoot := flag.String("sourcedir", "", "root of corresponding source directory")
	xattr := flag.Bool("xattr", true, "cache hashes in filesystem attribute.")

	flag.Parse()

	if *logfile != "" {
		f, err := os.OpenFile(*logfile, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
		if err != nil {
			log.Fatal("Could not open log file.", err)
		}
		log.Println("Log output to", *logfile)
		log.SetOutput(f)
	} else {
		log.SetPrefix("M")
	}

	secret, err := ioutil.ReadFile(*secretFile)
	if err != nil {
		log.Fatal("ReadFile", err)
	}

	excludeList := strings.Split(*exclude, ",")
	root, sock := absSocket(*socket)

	opts := termite.MasterOptions{
		Secret:       secret,
		MaxJobs:      *jobs,
		Excludes:     excludeList,
		Coordinator:  *coordinator,
		SourceRoot:   *srcRoot,
		WritableRoot: root,
		Paranoia:     *paranoia,
		Period:       time.Duration(*houseHoldPeriod * float64(time.Second)),
		KeepAlive:    time.Duration(*keepAlive * float64(time.Second)),
		FetchAll:     *fetchAll,
		StoreOptions: cba.StoreOptions{
			Dir: *cachedir,
		},
		RetryCount: *retry,
		XAttrCache: *xattr,
		LogFile:    *logfile,
		Socket:     sock,
	}
	master := termite.NewMaster(&opts)

	log.Println(termite.Version())

	go master.ServeHTTP(*port)
	master.Start()
}

func absSocket(sock string) (root, absSock string) {
	absSock, err := filepath.Abs(sock)
	if err != nil {
		log.Fatal("abs", err)
	}

	fi, err := os.Stat(absSock)
	if fi != nil && fi.Mode()&os.ModeSocket != 0 {
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
