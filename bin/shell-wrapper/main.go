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

func TryRunLocally(command string, topdir string) *os.Waitmsg {
	decider := termite.LocalDecider(topdir)
	if len(os.Args) == 3 && os.Args[0] == _SHELL && os.Args[1] == "-c" &&
		decider.ShouldRunLocally(command) {
		proc, err := os.StartProcess(_SHELL, os.Args, &os.ProcAttr{
		Files: []*os.File{os.Stdin, os.Stdout, os.Stderr},
		})
		if err != nil {
			log.Fatalf("os.StartProcess() for %s: %v", command, err)
		}
		msg, err := proc.Wait(0)
		if err != nil {
			log.Fatalf("proc.Wait() for %s: %v", command, err)
		}
		return msg
	}

	return nil
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

	localWaitMsg := TryRunLocally(*command, topDir)
	if localWaitMsg != nil {
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
		Env:    os.Environ(),
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
