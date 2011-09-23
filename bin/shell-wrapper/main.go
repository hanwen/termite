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

const _SHELL = "/bin/sh"

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
	socket := termite.FindSocket()
	conn := termite.OpenSocketConnection(socket, termite.RPC_CHANNEL)

	client := rpc.NewClient(conn)

	req := 1
	rep := 1
	err := client.Call("LocalMaster.RefreshAttributeCache", &req, &rep)
	client.Close()
	if err != nil {
		log.Fatal("LocalMaster.RefreshAttributeCache: ", err)
	}
	conn.Close()
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
	socket := termite.FindSocket()
	conn := termite.OpenSocketConnection(socket, termite.RPC_CHANNEL)
	client := rpc.NewClient(conn)
	wd, _ := os.Getwd()
	for _, p := range files {
		if p[0] != '/' {
			p = filepath.Join(wd, p)
		}

		req := termite.AttrRequest{Name: p}
		rep := termite.AttrResponse{}
		err := client.Call("LocalMaster.InspectFile", &req, &rep)
		if err != nil {
			log.Fatal("LocalMaster.InspectFile: ", err)
		}

		for _, a := range rep.Attrs {
			log.Printf("%v", *a)
		}
	}
}

func TryRunLocally(command string, topdir string) (exit *os.Waitmsg, rule termite.LocalRule) {
	decider := termite.NewLocalDecider(topdir)
	if !(len(os.Args) == 3 && os.Args[0] == _SHELL && os.Args[1] == "-c") {
		return
	}

	rule = decider.ShouldRunLocally(command)
	if rule.Local {
		env := os.Environ()
		if !rule.Recurse {
			env = cleanEnv(env)
		}

		proc, err := os.StartProcess(_SHELL, os.Args, &os.ProcAttr{
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
	inspect := flag.Bool("inspect", false, "inspect files on master.")
	debug := flag.Bool("dbg", false, "set on debugging in request.")
	flag.Parse()

	if *refresh {
		Refresh()
	}

	if *inspect {
		Inspect(flag.Args())
	}

	if *command == "" {
		return
	}
	os.Args[0] = _SHELL
	TryRunDirect(*command)

	socket := termite.FindSocket()
	if socket == "" {
		log.Fatal("Could not find .termite-socket")
	}
	topDir, _ := filepath.Split(socket)

	localWaitMsg, localRule := TryRunLocally(*command, topDir)
	if localWaitMsg != nil && !localRule.SkipRefresh {
		Refresh()
	}

	wd, err := os.Getwd()
	if err != nil {
		log.Fatal("Getwd", err)
	}

	conn := termite.OpenSocketConnection(socket, termite.RPC_CHANNEL)

	// TODO - could skip the shell if we can deduce it is a
	// no-frills command invocation.
	req := termite.WorkRequest{
		Binary:     _SHELL,
		Argv:       []string{"/bin/sh", "-c", *command},
		Env:        cleanEnv(os.Environ()),
		Dir:        wd,
		RanLocally: localWaitMsg != nil,
	}
	req.Debug = localRule.Debug || os.Getenv("TERMITE_DEBUG") != "" || *debug
	client := rpc.NewClient(conn)

	rep := termite.WorkResponse{}
	err = client.Call("LocalMaster.Run", &req, &rep)
	client.Close()
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

	conn.Close()
	os.Exit(localWaitMsg.ExitStatus())
}
