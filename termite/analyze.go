package termite

import (
	"crypto"
	_ "crypto/md5"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/hanwen/termite/analyze"
)

func DumpAnnotations(req *WorkRequest, rep *WorkResponse, start time.Time,
	outDir string, topDir string) {
	dur := time.Since(start)
	a := analyze.Command{
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

	if err := ioutil.WriteFile(filepath.Join(outDir, fn), out, 0644); err != nil {
		log.Fatalf("WriteFile: %v", err)
	}
}
