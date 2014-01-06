package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/signal"
	"os/user"
	"runtime"
	"strconv"
	"syscall"

	"github.com/hanwen/termite/cba"
	"github.com/hanwen/termite/termite"
)

var _ = log.Printf

func handleStop(daemon *termite.Worker) {
	ch := make(chan os.Signal, 1)

	signal.Notify(ch, os.Interrupt, os.Kill)
	for sig := range ch {
		log.Println("got signal: ", sig)
		req := termite.ShutdownRequest{Kill: true}
		rep := termite.ShutdownResponse{}
		daemon.Shutdown(&req, &rep)
	}
}

func OpenUniqueLog(base string) *os.File {
	name := base
	i := 0
	for {
		fi, _ := os.Stat(name)
		if fi == nil {
			break
		}

		name = fmt.Sprintf("%s.%d", base, i)
		i++
	}

	f, err := os.OpenFile(name, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		log.Fatal("Could not open log file.", err)
	}
	return f
}

func main() {
	version := flag.Bool("version", false, "print version and exit.")
	cachedir := flag.String("cachedir", "/var/cache/termite/worker-cache", "content cache")
	tmpdir := flag.String("tmpdir", "/var/tmp",
		"where to create FUSE mounts; should be on same partition as cachedir.")
	secretFile := flag.String("secret", "secret.txt", "file containing password.")
	port := flag.Int("port", 1232, "Start of port to try.")
	portRetry := flag.Int("port-retry", 10, "How many other ports to try.")
	coordinator := flag.String("coordinator", "", "Where to register the worker.")
	jobs := flag.Int("jobs", 1, "Max number of jobs to run.")
	reapcount := flag.Int("reap-count", 1, "Number of jobs per filesystem.")
	userFlag := flag.String("user", "nobody", "Run as this user.")
	logfile := flag.String("logfile", "", "Output log file to use.")
	stderrFile := flag.String("stderr", "", "File to write stderr output to.")
	paranoia := flag.Bool("paranoia", false, "Check attribute cache.")
	cpus := flag.Int("cpus", 1, "Number of CPUs to use.")
	heap := flag.Int("heap-size", 0, "Maximum heap size in MB.")
	flag.Parse()

	if *version {
		log.Println(termite.Version())
		os.Exit(0)
	}

	if os.Geteuid() != 0 {
		log.Fatal("This program must run as root")
	}
	secret, err := ioutil.ReadFile(*secretFile)
	if err != nil {
		log.Fatal("ReadFile", err)
	}

	if *logfile != "" {
		f := OpenUniqueLog(*logfile)
		log.Println("Log output to", *logfile)
		log.SetOutput(f)
	} else {
		log.SetPrefix("W")
	}

	if *stderrFile != "" {
		f := OpenUniqueLog(*stderrFile)
		err = syscall.Close(2)
		if err != nil {
			log.Fatalf("close stderr: %v", err)
		}
		_, err = syscall.Dup(int(f.Fd()))
		if err != nil {
			log.Fatalf("dup: %v", err)
		}
		f.Close()
	}

	opts := termite.WorkerOptions{
		Secret:      secret,
		TempDir:     *tmpdir,
		Jobs:        *jobs,
		Paranoia:    *paranoia,
		ReapCount:   *reapcount,
		LogFileName: *logfile,
		StoreOptions: cba.StoreOptions{
			Dir: *cachedir,
		},
		HeapLimit:   uint64(*heap) * (1 << 20),
		Coordinator: *coordinator,
		Port:        *port,
		PortRetry:   *portRetry,
	}
	if os.Geteuid() == 0 {
		nobody, err := user.Lookup(*userFlag)
		if err != nil {
			log.Fatalf("can't lookup %q: %v", *userFlag, err)
		}
		uid, err := strconv.ParseInt(nobody.Uid, 10, 64)
		gid, err := strconv.ParseInt(nobody.Gid, 10, 64)
		opts.User = &termite.User{
			Uid: int(uid),
			Gid: int(gid),
		}
	}

	daemon := termite.NewWorker(&opts)
	if *cpus > 0 {
		runtime.GOMAXPROCS(*cpus)
	}
	log.Printf("%s on %d CPUs", termite.Version(), runtime.GOMAXPROCS(0))
	go handleStop(daemon)
	daemon.RunWorkerServer()
}
