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

	annotations := map[string][]*analyze.Annotation{}
	for _, r := range results {
		annotations[r.Target] = append(annotations[r.Target], r)
	}

	actions := map[string]*analyze.Action{}
	for _, a := range annotations {
		action := analyze.CombineCommands(a)
		for k := range action.Writes {
			if v, ok := actions[k]; ok {
				if v != action {
					log.Printf("ignoring duplicate write %q: %v   %v", k, v.Annotations, action.Annotations)
				}
			} else {
				actions[k] = action
			}
		}
		for k := range action.Targets {
			if v, ok := actions[k]; ok {
				if v != action {
					log.Printf("ignoring duplicate target %q: have %v, add %v", k, v, action)
				}
			} else {
				actions[k] = action
			}
		}
	}

	log.Printf("serving on %s", *addr)
	analyze.ServeAction(*addr, actions)
}
