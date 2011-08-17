package termite

import (
	"bytes"
	"bufio"
	"io"
	"log"
	"os"
	"path/filepath"
	"regexp"
)

type localDeciderRule struct {
	*regexp.Regexp
	runLocal bool
	refresh  bool // TODO - implement this
}

type localDecider struct {
	rules []localDeciderRule
}

// TODO - use json to encode this instead.
func newLocalDecider(input io.Reader) *localDecider {
	decider := localDecider{}
	reader := bufio.NewReader(input)

	for {
		line, _, err := reader.ReadLine()
		if err == os.EOF {
			break
		}
		if len(line) == 0 {
			continue
		}
		runRemote := line[0] == '-'
		if runRemote {
			line = line[1:]
		}

		re, err := regexp.Compile(string(line))
		if err != nil {
			log.Printf("error in regexp.Compile for %q:", string(line), err)
			return nil
		}

		r := localDeciderRule{
			Regexp:   re,
			runLocal: !runRemote,
		}

		decider.rules = append(decider.rules, r)
	}
	return &decider
}

func (me *localDecider) shouldRunLocally(command string) bool {
	for _, r := range me.rules {
		if r.Regexp.MatchString(command) {
			return r.runLocal
		}
	}

	return false
}

const defaultLocal = (".*termite-make\n" +
	".*/cmake\n" +
	"-.*\n")

func (me *Master) setLocalDecider() {
	localRc := filepath.Join(me.writableRoot, ".termite-localrc")

	f, _ := os.Open(localRc)
	if f != nil {
		me.localDecider = newLocalDecider(f)
		f.Close()
	}

	if me.localDecider == nil {
		me.localDecider = newLocalDecider(bytes.NewBufferString(defaultLocal))
	}
}

func (me *Master) shouldRunLocally(req *WorkRequest) bool {
	// TODO - softcode /bin/sh.
	return len(req.Argv) == 3 && req.Argv[0] == "/bin/sh" && req.Argv[1] == "-c" &&
		me.localDecider.shouldRunLocally(req.Argv[2])
}
