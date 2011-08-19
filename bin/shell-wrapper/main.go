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

const _SHELL = "/bin/sh"

func TryRunDirect(cmd string) {
	parsed := termite.ParseCommand(cmd)
	if len(parsed) > 0 && parsed[0] == "echo" {
		fmt.Println(strings.Join(parsed[1:], " "))
		os.Exit(0)
	}
	// TODO mkdir, rm, others?
}

func Refresh() {
	socket := termite.FindSocket()
	conn := termite.OpenSocketConnection(socket, termite.RPC_CHANNEL)

	client := rpc.NewClient(conn)

	req := 1
	rep := 1
	err := client.Call("LocalMaster.RefreshAttributeCache", &req, &rep)
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
		Env: env,
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
	flag.Parse()

	if *refresh {
		Refresh()
	}

	if *command == "" {
		return
	}
	os.Args[0] = _SHELL
	TryRunDirect(*command)

	socket := termite.FindSocket()
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
		Binary: _SHELL,
		Argv:   os.Args,
		Env:    cleanEnv(os.Environ()),
		Dir:    wd,
		Debug:  os.Getenv("TERMITE_DEBUG") != "",
		RanLocally: localWaitMsg != nil,
	}
	client := rpc.NewClient(conn)

	rep := termite.WorkReply{}
	err = client.Call("LocalMaster.Run", &req, &rep)
	if err != nil {
		log.Fatal("LocalMaster.Run: ", err)
	}

	os.Stdout.Write([]byte(rep.Stdout))
	os.Stderr.Write([]byte(rep.Stderr))

	// TODO -something with signals.
	if localWaitMsg == nil {
		localWaitMsg = rep.Exit
	}
	os.Exit(localWaitMsg.ExitStatus())
}
