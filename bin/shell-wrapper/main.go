package main

import (
	"flag"
	"github.com/hanwen/termite/termite"
	"log"
	"os"
	"rpc"
	"strings"
)

/*
 Considerations:

 * should be more generic

 * need to be careful, since we can't detect changes to local files
   if we execute a local recipe, eg.

    echo foo > file

   must be distributed.

 * Could alias echo, mkdir and some other simple recipes locally,
   especially if we can infer the effects on the filesystem.

*/
func RunLocally(cmd string) bool {
	// TODO - use regex.
	if strings.Index(cmd, "make") >= 0 {
		return true
	}

	// TODO - more? see above.
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
