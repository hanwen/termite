package main

import (
	"flag"
	"github.com/hanwen/termite/termite"
	"log"
	"os"
	"path/filepath"
	"rpc"
	"strings"
)

func RunLocally(cmd string) bool {
	if strings.Index(cmd, "make")  >= 0 {
		return true
	}

	// TODO - split on arbitrary whitespace, trim leading
	// whitespace.
	first := strings.Split(cmd, " ")[0]

	// TODO - use more generic patterns to do this.
	//
	// TODO - detect simple invocations (no shell magic), and
	// skip the shell and/or run directly from here.
	switch first {
	case "echo":
		return true
	case "mkdir":
		return true
	}

	base := filepath.Base(first)
	switch base {
	case "make":
		return true
	}
	return false
}

const _SHELL = "/bin/sh"

func main() {
	command := flag.String("c", "", "command to run.")
	flag.Parse()

	if *command == "" {
		return
	}

	os.Args[0] = _SHELL
	if RunLocally(*command) {
		if err := os.Exec(_SHELL, os.Args, os.Environ()); err != nil {
			log.Fatal("exec", err)
		}
	}

	wd, err := os.Getwd()
	if err != nil {
		log.Fatal("Getwd", err)
	}

	socket := termite.FindSocket()
	conn := termite.OpenSocketConnection(socket, termite.RPC_CHANNEL)
	req := termite.WorkRequest{
		Binary:  _SHELL,
		Argv:    os.Args,
		Env:     os.Environ(),
		Dir:     wd,
		Debug:   os.Getenv("TERMITE_DEBUG") != "",
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
	os.Exit(rep.Exit.ExitStatus())
}
