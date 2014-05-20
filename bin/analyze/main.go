package main

import (
	"flag"
	"log"
	"regexp"

	"termite/analyze"
)

func main() {
	addr := flag.String("addr", ":8080", "address to serve on")
	depReStr := flag.String("dep_re", "", "file name regexp for dependency files")
	flag.Parse()
	dir := flag.Arg(0)

	var re *regexp.Regexp
	if *depReStr != "" {
		re = regexp.MustCompile(*depReStr)
	}
	results, err := analyze.ReadDir(dir, re)
	if err != nil {
		log.Fatal(err)
	}

	gr := analyze.NewGraph(results)

	log.Printf("serving on %s", *addr)
	if err := gr.Serve(*addr); err != nil {
		log.Printf("serve: %v", err)
	}
}
