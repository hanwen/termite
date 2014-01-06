package main

import (
	"flag"
	"io/ioutil"
	"log"
	"net"
	"syscall"

	"github.com/hanwen/termite/cba"
	"github.com/hanwen/termite/termite"
)

func main() {
	// coordinator := flag.String("coordinator", "localhost:1230", "address of coordinator. Overrides -workers")
	secretFile := flag.String("secret", "secret.txt", "file containing password.")
	cachedir := flag.String("cachedir", "/var/cache/termite/worker-cache", "content cache")
	port := flag.Int("port", 0, "RPC port")

	flag.Parse()

	secret, err := ioutil.ReadFile(*secretFile)
	if err != nil {
		log.Fatal("ReadFile", err)
	}
	opts := cba.StoreOptions{
		Dir: *cachedir,
	}
	store := cba.NewStore(&opts)
	listener := termite.AuthenticatedListener(*port, secret, 10)
	for {
		conn, err := listener.Accept()
		if err == syscall.EINVAL {
			break
		}
		if err != nil {
			if e, ok := err.(*net.OpError); ok && e.Err == syscall.EINVAL {
				break
			}
			log.Println("me.listener", err)
			break
		}

		log.Println("Authenticated connection from", conn.RemoteAddr())
		go store.ServeConn(conn)
	}
}
