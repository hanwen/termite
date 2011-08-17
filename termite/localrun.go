package termite

import (
	"bufio"
	"io"
	"json"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

type localDeciderRule struct {
	Regexp string
	Local bool
}

type localDecider struct {
	rules []localDeciderRule
}

func newLocalDecider(input io.Reader) *localDecider {
	reader := bufio.NewReader(input)
	out := []byte{}
	for {
		line, _, err := reader.ReadLine()
		if err != nil {
			break
		}
		if len(line) == 0 || line[0] == '#' || strings.HasPrefix(string(line), "//") {
			continue
		}
		out = append(out, line...)
		out = append(out, '\n')
	}

	decider := localDecider{}
	err := json.Unmarshal([]byte(out), &decider.rules)
	if err != nil {
		log.Println(err)
		return nil
	}
	return &decider
}

func (me *localDecider) ShouldRunLocally(command string) bool {
	for _, r := range me.rules {
		m, err := regexp.MatchString(r.Regexp, command)
		if err != nil {
			log.Println("regexp error:", err)
			continue
		}
		if m {
			return r.Local
		}
	}

	return false
}

func LocalDecider(dir string) *localDecider {
	localRc := filepath.Join(dir, ".termite-localrc")

	f, _ := os.Open(localRc)
	if f != nil {
		defer f.Close()
		return newLocalDecider(f)
	}

	rules := []localDeciderRule{
		localDeciderRule{Regexp: ".*termite-make", Local: true},
		localDeciderRule{Regexp: ".*", Local: false},
	}
	return &localDecider{rules}
}
