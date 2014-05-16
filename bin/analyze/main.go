package main

import (
	"flag"
	"log"

	"termite/analyze"
)

func main() {
	addr := flag.String("addr", ":8080", "address to serve on")
	flag.Parse()
	dir := flag.Arg(0)

	results, err := analyze.ReadDir(dir)
	if err != nil {
		log.Fatal(err)
	}

	gr := analyze.NewGraph(results)

	log.Printf("serving on %s", *addr)
	if err := gr.Serve(*addr); err != nil {
		log.Printf("serve: %v", err)
	}
}
