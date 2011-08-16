package main

import (
	"flag"
	"fmt"
	"github.com/hanwen/termite/termite"
	"http"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"rpc"
)

func serveBin(name string) func(w http.ResponseWriter, req *http.Request) {
	return func(w http.ResponseWriter, req *http.Request) {
		me, _ := os.Readlink("/proc/self/exe")
		d, _ := filepath.Split(me)

		for _, n := range []string{
			filepath.Join(d, name),
			filepath.Join(d, fmt.Sprintf("../%s/%s", name, name)),
		} {
			if fi, _ := os.Lstat(n); fi != nil && fi.IsRegular() {
				http.ServeFile(w, req, n)
			}
		}
	}
}

func main() {
	port := flag.Int("port", 1233, "Where to listen for work requests.")
	secretFile := flag.String("secret", "secret.txt", "file containing password.")
	flag.Parse()

	secret, err := ioutil.ReadFile(*secretFile)
	if err != nil {
		log.Fatal("ReadFile", err)
	}

	c := termite.NewCoordinator(secret)
	c.HandleHTTP()
	http.HandleFunc("/bin/chroot", serveBin("chroot"))
	http.HandleFunc("/bin/worker", serveBin("worker"))
	http.HandleFunc("/bin/shell-wrapper", serveBin("shell-wrapper"))
	http.HandleFunc("/bin/wrapper", serveBin("wrapper"))

	rpc.Register(c)
	rpc.HandleHTTP()

	go c.PeriodicCheck()
	addr := fmt.Sprintf(":%d", *port)
	log.Println("Listening on", addr)
	err = http.ListenAndServe(addr, nil)
	if err != nil {
		log.Fatal("ListenAndServe: ", err.String())
	}
}
