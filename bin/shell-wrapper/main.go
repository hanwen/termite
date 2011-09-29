package main

import (
	"flag"
	"fmt"
	"github.com/hanwen/termite/termite"
	"log"
	"os"
	"path/filepath"
	"rpc"
	"strings"
)

// TODO - this file is a mess. Clean it up.
const _TIMEOUT = 10e9
var socketRpc  *rpc.Client
var topDir     string
func Rpc() *rpc.Client {
	if socketRpc == nil {
		socket := termite.FindSocket()
		if socket == "" {
			log.Fatal("Could not find .termite-socket")
		}
		topDir, _ = filepath.Split(socket)
		topDir = filepath.Clean(topDir)
		conn := termite.OpenSocketConnection(socket, termite.RPC_CHANNEL, _TIMEOUT)
		socketRpc = rpc.NewClient(conn)
	}
	return socketRpc
}

func TryRunDirect(cmd string) {
	if cmd == ":" {
		os.Exit(0)
	}

	parsed := termite.ParseCommand(cmd)
	if len(parsed) == 0 {
		return
	}

	if parsed[0] == "echo" {
		fmt.Println(strings.Join(parsed[1:], " "))
		os.Exit(0)
	}
	if parsed[0] == "true" {
		os.Exit(0)
	}
	if parsed[0] == "false" {
		os.Exit(1)
	}

	// TODO mkdir, rm, expr, others?
}

func Refresh() {
	req := 1
	rep := 1
	err := Rpc().Call("LocalMaster.RefreshAttributeCache", &req, &rep)
	if err != nil {
		log.Fatal("LocalMaster.RefreshAttributeCache: ", err)
	}
}

func cleanEnv(input []string) []string {
	env := []string{}
	for _, v := range input {
		comps := strings.SplitN(v, "=", 2)
		if comps[1] == "termite-make" {
			// TODO - more generic.
			v = fmt.Sprintf("%s=%s", comps[0], "make")
		} else if comps[0] == "MAKE_SHELL" {
			continue
		}
		env = append(env, v)
	}
	return env
}

func Inspect(files []string) {
	wd, _ := os.Getwd()
	for _, p := range files {
		if p[0] != '/' {
			p = filepath.Join(wd, p)
		}

		req := termite.AttrRequest{Name: p}
		rep := termite.AttrResponse{}
		err := Rpc().Call("LocalMaster.InspectFile", &req, &rep)
		if err != nil {
			log.Fatal("LocalMaster.InspectFile: ", err)
		}

		for _, a := range rep.Attrs {
			log.Printf("%v", *a)
		}
	}
}

func Shell() string {
	shell := os.Getenv("SHELL")
	if shell == "" {
		shell = "/bin/sh"
	}
	return shell
}

func TryRunLocally(command string, topdir string) (exit *os.Waitmsg, rule termite.LocalRule) {
	decider := termite.NewLocalDecider(topdir)
	if !(len(os.Args) == 3 && os.Args[0] == Shell() && os.Args[1] == "-c") {
		return
	}

	rule = decider.ShouldRunLocally(command)
	if rule.Local {
		env := os.Environ()
		if !rule.Recurse {
			env = cleanEnv(env)
		}

		proc, err := os.StartProcess(Shell(), os.Args, &os.ProcAttr{
			Env:   env,
			Files: []*os.File{os.Stdin, os.Stdout, os.Stderr},
		})
		if err != nil {
			log.Fatalf("os.StartProcess() for %s: %v", command, err)
		}
		msg, err := proc.Wait(0)
		if err != nil {
			log.Fatalf("proc.Wait() for %s: %v", command, err)
		}
		return msg, rule
	}

	return
}

func main() {
	command := flag.String("c", "", "command to run.")
	refresh := flag.Bool("refresh", false, "refresh master file cache.")
	shutdown := flag.Bool("shutdown", false, "shutdown master.")
	inspect := flag.Bool("inspect", false, "inspect files on master.")
	exec := flag.Bool("exec", false, "run command args without shell.")
	directory := flag.String("dir", "", "directory from where to run (default: cwd).")
	
	debug := flag.Bool("dbg", false, "set on debugging in request.")
	flag.Parse()

	if *shutdown {
		req := 1
		rep := 1
		err := Rpc().Call("LocalMaster.Shutdown", &req, &rep)
		if err != nil {
			log.Fatal(err)
		}
	}
	if *refresh {
		Refresh()
	}

	if *inspect {
		Inspect(flag.Args())
	}

	if *directory == "" {
		wd, err := os.Getwd()
		if err != nil {
			log.Fatal("Getwd", err)
		}

		directory = &wd
	}
	
	os.Args[0] = Shell()
	if *command != "" {
		TryRunDirect(*command)
	}

	localWaitMsg, localRule := TryRunLocally(*command, topDir)
	if localWaitMsg != nil && !localRule.SkipRefresh {
		Refresh()
	}

	// TODO - could skip the shell if we can deduce it is a
	// no-frills command invocation.
	req := termite.WorkRequest{
		Binary:     Shell(),
		Argv:       []string{Shell(), "-c", *command},
		Env:        cleanEnv(os.Environ()),
		Dir:        *directory,
		RanLocally: localWaitMsg != nil,
	}
	if *exec {
		req.Binary = flag.Args()[0]
		req.Argv = flag.Args()
		log.Println(req)
	}
	
	req.Debug = localRule.Debug || os.Getenv("TERMITE_DEBUG") != "" || *debug
	rep := termite.WorkResponse{}
	err := Rpc().Call("LocalMaster.Run", &req, &rep)
	if err != nil {
		log.Fatal("LocalMaster.Run: ", err)
	}

	os.Stdout.Write([]byte(rep.Stdout))
	os.Stderr.Write([]byte(rep.Stderr))

	// TODO -something with signals.
	if localWaitMsg == nil {
		localWaitMsg = &rep.Exit
		if localWaitMsg.ExitStatus() != 0 {
			log.Printf("Failed: %q", *command)
		}
	}

	// TODO - is this necessary?
	Rpc().Close()
	os.Exit(localWaitMsg.ExitStatus())
}
