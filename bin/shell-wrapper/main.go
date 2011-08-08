package main

import (
	"bytes"
	"flag"
	"fmt"
	"github.com/hanwen/termite/termite"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"rpc"
	"strings"
)

const defaultLocal = (
	".*termite-make\n" +
	".*/cmake\n" +
	"-.*\n" )

func RunLocally(cmd, dir string) bool {
	content, err := ioutil.ReadFile(filepath.Join(dir, ".termite-localrc"))
	
	buf := bytes.NewBuffer(content)
	if err != nil {
		buf = bytes.NewBufferString(defaultLocal)
	}

	local := termite.MatchAgainst(buf, cmd)
	return local
}

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
	dir, _ := filepath.Split(socket)
	
	if RunLocally(*command, dir) {
		if err := os.Exec(_SHELL, os.Args, os.Environ()); err != nil {
			log.Fatal("exec", err)
		}
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
