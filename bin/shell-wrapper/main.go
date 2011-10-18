package main

import (
	"exec"
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

var socketRpc *rpc.Client
var topDir string

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

func TryRunDirect(req *termite.WorkRequest) {
	if req.Argv[0] == "echo" {
		fmt.Println(strings.Join(req.Argv[1:], " "))
		os.Exit(0)
	}
	if req.Argv[0] == "true" {
		os.Exit(0)
	}
	if req.Argv[0] == "false" {
		os.Exit(1)
	}
}

var bashInternals = []string{
	"alias", "bg", "bind", "break", "builtin", "caller", "case", "cd",
	"command", "compgen", "complete", "compopt", "continue", "coproc",
	"declare", "dirs", "disown", /* echo, */ "enable", "eval", "exec", "exit",
	"export", "false", "fc", "fg", "for", "for", "function", "getopts",
	"hash", "help", "history", "if", "jobs", "kill", "let", "local",
	"logout", "mapfile", "popd", "printf", "pushd", "pwd", "read",
	"readarray", "readonly", "return", "select", "set", "shift", "shopt",
	"source", "suspend", "test", "time", "times", "trap", "true", "type",
	"typeset", "ulimit", "umask", "unalias", "unset", "until",
	"variables", "wait", "while",
}

func NewWorkRequest(cmd string, dir string, topdir string) *termite.WorkRequest {
	req := &termite.WorkRequest{
		Binary: Shell(),
		Argv:   []string{Shell(), "-c", cmd},
		Env:    cleanEnv(os.Environ()),
		Dir:    dir,
	}

	parsed := termite.ParseCommand(cmd)
	if len(parsed) > 0 {
		// Is this really necessary?
		for _, c := range bashInternals {
			if parsed[0] == c {
				return req
			}
		}

		// A no-frills command invocation: do it directly.
		binary, err := exec.LookPath(parsed[0])
		if err == nil {
			req.Argv = parsed
			if len(binary) > 0 && binary[0] != '/' {
				binary = filepath.Join(req.Dir, binary)
			}
			req.Binary = binary
		}
	}

	return req
}

func PrepareRun(cmd string, dir string, topdir string) (*termite.WorkRequest, *termite.LocalRule) {
	cmd = termite.MakeUnescape(cmd)
	if cmd == ":" || strings.TrimRight(cmd, " ") == "" {
		os.Exit(0)
	}

	req := NewWorkRequest(cmd, dir, topdir)
	TryRunDirect(req)

	decider := termite.NewLocalDecider(topdir)
	rule := decider.ShouldRunLocally(cmd)
	if rule != nil {
		req.Debug = rule.Debug
		return req, rule
	}

	return req, nil
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
		p = p[1:]
		req := termite.AttrRequest{Name: p}
		rep := termite.AttrResponse{}
		err := Rpc().Call("LocalMaster.InspectFile", &req, &rep)
		if err != nil {
			log.Fatal("LocalMaster.InspectFile: ", err)
		}

		for _, a := range rep.Attrs {
			log.Printf("%v", *a)
			for n, m := range a.NameModeMap {
				log.Printf(" %q: %o", n, m)
			}
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

func RunLocally(req *termite.WorkRequest, rule *termite.LocalRule) *os.Waitmsg {
	env := os.Environ()
	if !rule.Recurse {
		env = cleanEnv(env)
	}

	proc, err := os.StartProcess(req.Binary, req.Argv, &os.ProcAttr{
		Env:   env,
		Files: []*os.File{os.Stdin, os.Stdout, os.Stderr},
	})
	if err != nil {
		log.Fatalf("os.StartProcess() for %v: %v", req, err)
	}
	msg, err := proc.Wait(0)
	if err != nil {
		log.Fatalf("proc.Wait() for %v: %v", req, err)
	}
	return msg
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
	log.SetPrefix("S")

	if *shutdown {
		req := 1
		rep := 1
		err := Rpc().Call("LocalMaster.Shutdown", &req, &rep)
		if err != nil {
			log.Fatal(err)
		}
		return
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

	var req *termite.WorkRequest
	var rule *termite.LocalRule
	if *exec {
		req = &termite.WorkRequest{
			Binary: flag.Args()[0],
			Argv:   flag.Args(),
			Dir:    *directory,
			Env:    os.Environ(),
		}
	} else {
		req, rule = PrepareRun(*command, *directory, topDir)
	}
	var waitMsg *os.Waitmsg
	if rule != nil && rule.Local {
		waitMsg = RunLocally(req, rule)
		if !rule.SkipRefresh {
			Refresh()
		}
	} else {
		req.Debug = req.Debug || os.Getenv("TERMITE_DEBUG") != "" || *debug
		rep := termite.WorkResponse{}
		err := Rpc().Call("LocalMaster.Run", &req, &rep)
		if err != nil {
			log.Fatal("LocalMaster.Run: ", err)
		}

		os.Stdout.Write([]byte(rep.Stdout))
		os.Stderr.Write([]byte(rep.Stderr))

		waitMsg = &rep.Exit
	}

	if waitMsg.ExitStatus() != 0 {
		log.Printf("Failed: '%q'", *command)
	}

	// TODO - is this necessary?
	Rpc().Close()
	os.Exit(waitMsg.ExitStatus())
}
