package analyze

import (
	"crypto"
	_ "crypto/md5"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/hanwen/termite/termite"
)

func DumpAnnotations(req *termite.WorkRequest, rep *termite.WorkResponse, dur time.Duration, topDir string) {
	d := filepath.Join(topDir, ".annotation")
	if fi, err := os.Stat(d); err != nil || !fi.IsDir() {
		if err := os.MkdirAll(d, 0755); err != nil {
			log.Fatalf("MkdirAll: %v", err)
		}
	}

	a := Command{
		Deps:    req.DeclaredDeps,
		Dir:     req.Dir,
		Target:  req.DeclaredTarget,
		Reads:   rep.Reads,
		Command: strings.Join(req.Argv, " "),
	}

	slashTopDir := topDir + "/"
	if rep.FileSet != nil {
		for _, f := range rep.Files {
			if f.IsDir() {
				continue
			}

			p := "/" + f.Path
			p = strings.TrimPrefix(p, slashTopDir)
			if f.Deletion() {
				a.Deletions = append(a.Deletions, p)
			} else {
				a.Writes = append(a.Writes, p)
			}
		}
	}
	sort.Strings(a.Deletions)
	sort.Strings(a.Writes)
	sort.Strings(a.Deps)
	sort.Strings(a.Reads)
	out, err := json.Marshal(&a)
	if err != nil {
		log.Fatalf("Marshal: %v", err)
	}
	h := crypto.MD5.New()
	if err != nil {
		log.Fatalf("MD5: %v", err)
	}
	h.Write(out)
	fn := fmt.Sprintf("%x", h.Sum(nil))

	// Don't hash timestamps.
	a.Time = time.Now()
	a.Duration = dur
	a.Filename = fn

	out, err = json.Marshal(&a)
	if err != nil {
		log.Fatalf("Marshal: %v", err)
	}

	out = append(out, '\n')

	if err := ioutil.WriteFile(filepath.Join(d, fn), out, 0644); err != nil {
		log.Fatalf("WriteFile: %v", err)
	}
}
