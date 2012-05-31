package cba

import (
	"fmt"
	"io/ioutil"
	"log"
	"regexp"

	"github.com/hanwen/termite/fastpath"
)

func ReadHexDatabase(d string) map[string]bool {
	hexRe := regexp.MustCompile("^([0-9a-fA-F][0-9a-fA-F])+$")
	db := map[string]bool{}
	entries, err := ioutil.ReadDir(d)
	if err != nil {
		return db
	}

	for _, e := range entries {
		if !hexRe.MatchString(e.Name()) || !e.IsDir() {
			continue
		}

		sub, _ := ioutil.ReadDir(fastpath.Join(d, e.Name()))
		for _, s := range sub {
			if !hexRe.MatchString(s.Name()) || s.IsDir() {
				continue
			}

			hex := e.Name() + s.Name()
			bin := make([]byte, len(hex)/2)
			n, err := fmt.Sscanf(hex, "%x", &bin)
			if n != 1 {
				log.Panic("sscanf %d %v", n, err)
			}

			db[string(bin)] = true
		}
	}

	return db
}
