package main

import (
	"github.com/hanwen/termite/termite"
	"io"
	"log"
	"os"
	"path/filepath"
	"rpc"
)

func main() {
	path := os.Getenv("PATH")

	args := os.Args
	_, base := filepath.Split(args[0])
	binary := ""
	self, err := filepath.EvalSymlinks("/proc/self/exe")
	if err == nil {
		self, err = filepath.Abs(self)
	}
	if err != nil {
		log.Fatal("EvalSymlinks", err)
	}

	for _, c := range filepath.SplitList(path) {
		try := filepath.Join(c, base)
		try,_ = filepath.EvalSymlinks(try)
		try,_ = filepath.Abs(try)
		if try == self {
			continue
		}

		fi, _ := os.Stat(try)
		if fi != nil && fi.IsRegular() {
			binary = try
		}
	}

	if binary == "" {
		log.Fatal("could not find", base)
	}

	wd, err := os.Getwd()
	if err != nil {
		log.Fatal("Getwd", err)
	}

	socket := termite.FindSocket()
	conn := termite.OpenSocketConnection(socket, termite.RPC_CHANNEL)
	args[0] = binary
	req := termite.WorkRequest{
		StdinId: termite.ConnectionId(),
		Binary:  binary,
		Argv:    args,
		Env:     os.Environ(),
		Dir:     wd,
		Debug:   os.Getenv("TERMITE_DEBUG") != "",
	}

	stdinConn := termite.OpenSocketConnection(socket, req.StdinId)
	go func() {
		io.Copy(stdinConn, os.Stdin)
		stdinConn.Close()
	}()
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
