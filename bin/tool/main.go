package main

import (
	"github.com/hanwen/go-fuse/termite"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"rpc"
)

const _SOCKET = ".termite-socket"

func OpenConn(socket string, channel string) net.Conn {
	conn, err := net.Dial("unix", socket)
	if err != nil {
		log.Fatal("Dial:", err)
	}
	if len(channel) != termite.HEADER_LEN {
		panic(channel)
	}
	_, err = io.WriteString(conn, channel)
	if err != nil {
		log.Fatal("WriteString", err)
	}
	return conn
}

const _TEST_CONNECTION = "test-termite-connection"

func main() {
	path := os.Getenv("PATH")

	args := os.Args
	_, base := filepath.Split(args[0])
	binary := ""
	for _, c := range filepath.SplitList(path) {
		_, dirBase := filepath.Split(c)
		if dirBase == "termite" {
			continue
		}

		try := filepath.Join(c, base)
		_, err := os.Stat(try)
		if err == nil {
			binary = try
		}
	}
	if binary == "" && binary != _TEST_CONNECTION {
		log.Fatal("could not find", base)
	}

	wd, err := os.Getwd()
	if err != nil { log.Fatal("Getwd", err) }

	socket := os.Getenv("TERMITE_SOCKET")
	if socket == "" {
		socketPath := wd
		for socketPath != "/" {
			cand := filepath.Join(socketPath, _SOCKET)
			fi, _ := os.Lstat(cand)
			if fi != nil && fi.IsSocket() {
				socket = cand
				break
			}
			socketPath = filepath.Clean(filepath.Join(socketPath, ".."))
		}
	}

	if binary == _TEST_CONNECTION {
		conn, err := net.Dial("unix", socket)

		exit := 0
		if err != nil {
			exit = 1
		} else {
			conn.Close()
		}
		os.Exit(exit)
	}

	conn := OpenConn(socket, termite.RPC_CHANNEL)
	args[0] = binary
	req := termite.WorkRequest{
		StdinId: termite.ConnectionId(),
		Binary:  binary,
		Argv:    args,
		Env:     os.Environ(),
		Dir:     wd,
	}

	stdinConn := OpenConn(socket, req.StdinId)
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
