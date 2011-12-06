package main

import (
	"flag"
	"fmt"
	"github.com/hanwen/termite/termite"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path/filepath"
)

func serveBin(name string) func(w http.ResponseWriter, req *http.Request) {
	return func(w http.ResponseWriter, req *http.Request) {
		me, _ := os.Readlink("/proc/self/exe")
		d, _ := filepath.Split(me)

		for _, n := range []string{
			filepath.Join(d, name),
			filepath.Join(d, fmt.Sprintf("../%s/%s", name, name)),
		} {
			if fi, _ := os.Lstat(n); fi != nil && !fi.IsDir() {
				http.ServeFile(w, req, n)
			}
		}
	}
}

func main() {
	port := flag.Int("port", 1233, "Where to listen for work requests.")
	secretFile := flag.String("secret", "secret.txt", "file containing password.")
	flag.Parse()
	log.SetPrefix("C")

	secret, err := ioutil.ReadFile(*secretFile)
	if err != nil {
		log.Fatal("ReadFile", err)
	}

	c := termite.NewCoordinator(secret)
	c.Mux.HandleFunc("/bin/worker", serveBin("worker"))
	c.Mux.HandleFunc("/bin/shell-wrapper", serveBin("shell-wrapper"))

	log.Println(termite.Version())
	go c.PeriodicCheck()
	c.ServeHTTP(*port)
}
