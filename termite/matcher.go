package termite

import (
	"bufio"
	"io"
	"log"
	"os"
	"regexp"
)

func MatchAgainst(rd io.Reader, input string) bool {
	reader := bufio.NewReader(rd)
	for {
		line, _, err := reader.ReadLine()
		if err == os.EOF {
			break
		}
		invert := len(line) > 0 && line[0] == '-'
		if invert {
			line = line[1:]
		}
		match, err := regexp.MatchString(string(line), input)
		if err != nil {
			log.Println("MatchString:", err)
		}
		if match {
			return !invert
		}
	}
	return false
}
