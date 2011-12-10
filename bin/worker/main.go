package main

import (
	"flag"
	"github.com/hanwen/termite/cba"
	"github.com/hanwen/termite/termite"
	"io/ioutil"
	"log"
	"os"
	"os/signal"
	"os/user"
	"runtime"
	"runtime/pprof"
	"syscall"
)

var _ = log.Printf

func handleStop(daemon *termite.Worker) {
	for {
		sig := <-signal.Incoming
		switch sig.(os.UnixSignal) {
		case syscall.SIGTERM, syscall.SIGQUIT, syscall.SIGINT:
			log.Println("got signal: ", sig)
			req := termite.ShutdownRequest{}
			rep := termite.ShutdownResponse{}
			daemon.Shutdown(&req, &rep)
		case syscall.SIGHUP:
			daemon.Report()
		}
	}
}

func main() {
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
	memcache := flag.Int("filecache", 1024, "number of <32k files to cache in memory")
	logfile := flag.String("logfile", "", "Output log file to use.")
	paranoia := flag.Bool("paranoia", false, "Check attribute cache.")
	cpus := flag.Int("cpus", 1, "Number of CPUs to use.")
	heap := flag.Int("heap-size", 0, "Maximum heap size in MB.")
	cpuprofile := flag.String("profile", "", "File to write profile output to.")
	flag.Parse()

	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	if os.Geteuid() != 0 {
		log.Fatal("This program must run as root")
	}
	secret, err := ioutil.ReadFile(*secretFile)
	if err != nil {
		log.Fatal("ReadFile", err)
	}

	if *logfile != "" {
		f, err := os.OpenFile(*logfile, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
		if err != nil {
			log.Fatal("Could not open log file.", err)
		}
		log.Println("Log output to", *logfile)
		log.SetOutput(f)
	} else {
		log.SetPrefix("W")
	}

	opts := termite.WorkerOptions{
		Secret:      secret,
		TempDir:     *tmpdir,
		Jobs:        *jobs,
		Paranoia:    *paranoia,
		ReapCount:   *reapcount,
		LogFileName: *logfile,
		ContentCacheOptions: cba.ContentCacheOptions{
			Dir:      *cachedir,
			MemCount: *memcache,
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
		opts.User = nobody
	}

	daemon := termite.NewWorker(&opts)
	if *cpus > 0 {
		runtime.GOMAXPROCS(*cpus)
	}
	log.Printf("%s on %d CPUs", termite.Version(), runtime.GOMAXPROCS(0))
	go handleStop(daemon)
	daemon.RunWorkerServer()
}
