package termite

import (
	"crypto"
	"fmt"
	"github.com/hanwen/go-fuse/fuse"
	"io"
	"log"
	"os"
	"path/filepath"
	"rand"
	"regexp"
	"strings"
	"time"
)

func PrintStdinSliceLen(s []byte) {
	log.Printf("Copied %d bytes of stdin", len(s))
}

// Useful for debugging.
func HookedCopy(w io.Writer, r io.Reader, proc func([]byte)) os.Error {
	buf := make([]byte, 32*1024)
	for {
		n, err := r.Read(buf)
		if n > 0 && proc != nil {
			proc(buf[:n])
		}
		todo := buf[:n]
		for len(todo) > 0 {
			n, err = w.Write(todo)
			if err != nil {
				break
			}
			todo = todo[n:]
		}
		if len(todo) > 0 {
			return err
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func RandomBytes(n int) []byte {
	c := make([]byte, 0)
	for i := 0; i < n; i++ {
		c = append(c, byte(rand.Int31n(256)))
	}
	return c
}

func init() {
	rand.Seed(time.Nanoseconds() ^ (int64(os.Getpid()) << 32))
}

func md5str(s string) []byte {
	h := crypto.MD5.New()
	io.WriteString(h, s)
	return h.Sum()
}

func md5(c []byte) []byte {
	h := crypto.MD5.New()
	h.Write(c)
	return h.Sum()
}

// Like io.Copy, but returns the buffer if it was small enough to hold
// of the copied bytes.
func SavingCopy(w io.Writer, r io.Reader, bufSize int) ([]byte, os.Error) {
	buf := make([]byte, bufSize)
	total := 0
	for {
		n, err := r.Read(buf)
		todo := buf[:n]
		total += n
		for len(todo) > 0 {
			n, err = w.Write(todo)
			if err != nil {
				break
			}
			todo = todo[n:]
		}
		if len(todo) > 0 {
			return nil, err
		}
		if err == os.EOF || n == 0 {
			break
		}
		if err != nil {
			return nil, err
		}
	}

	if total < cap(buf) {
		return buf[:total], nil
	}
	return nil, nil
}

// Argument ordering follows io.Copy.
func CopyFile(dstName string, srcName string, mode int) os.Error {
	src, err := os.Open(srcName)
	if err != nil {
		return err
	}
	defer src.Close()

	dst, err := os.OpenFile(dstName, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, uint32(mode))
	if err != nil {
		return err
	}
	defer dst.Close()

	_, err = io.Copy(dst, src)

	if err == os.EOF {
		err = nil
	}
	return err
}

func Version() string {
	tVersion := "unknown"
	if version != nil {
		tVersion = *version
	}

	return fmt.Sprintf("Termite %s (go-fuse %s)",
		tVersion, fuse.Version())
}

func EscapeRegexp(s string) string {
	special := "[]()\\+*"
	for i, _ := range special {
		c := special[i:i+1]
		s = strings.Replace(s, c, "\\" + c, -1)
	}
	return s
}

func DetectFiles(root string, cmd string) []string {
	regexp, err := regexp.Compile("(" + EscapeRegexp(root) + "/[^ ;&|\"']*)")
	if err != nil {
		log.Println("regexp error", err)
	}

	names := []string{}
	matches := regexp.FindAllString(cmd, -1)
	for _, m := range matches {
		names = append(names, m)
	}
	return names
}


func IsSpace(b byte) bool {
	return b == ' ' || b == '\n' || b == '\f' || b == '\t'
}

var controlCharMap = map[byte]bool {
	'$': true,
	'>': true,
	'<': true,
	'&': true,
	'|': true,
	';': true,
	'*': true,
	'?': true,
	// TODO - [] function as wildcards, but let's slip this through
	// rather than patching up the LLVM compile.
	//	'[': true,
	//	']': true,
	'(': true,
	')': true,
	'{': true,
	'}': true,
	'~': true,
	'`': true,
}

// ParseCommand tries to parse quoting for a shell command line.  It
// will give up and return nil when it returns shell-metacharacters
// ($, ` , etc.)
func ParseCommand(cmd string) []string {
	escape := false
	squote := false
	dquote := false

	result := []string{}
	word := []byte{}
	for i, ch := range cmd {
		c := byte(ch)
		if squote {
			if c == '\'' {
				squote = false
			} else {
				word = append(word, c)
			}
			continue
		}
		if dquote {
			// TODO - not really correct; "a\nb" -> a\nb
			if escape {
				word = append(word, byte(c))
				escape = false
				continue
			}

			switch c {
			case '"':
				dquote = !dquote
			case '\\':
				escape = true
			case '$':
				return nil
			default:
				word = append(word, c)
			}
			continue
		}
		if escape {
			word = append(word, c)
			escape = false
			continue
		}
		if c == '\'' {
			squote = true
			continue
		}
		if c == '"' {
			dquote = true
			continue
		}
		if c == '\\' {
			escape = true
			continue
		}
		if controlCharMap[c] {
			return nil
		}
		if IsSpace(c) {
			if i > 0 && !IsSpace(cmd[i-1]) {
				result = append(result, string(word))
				word = []byte{}
			}
		} else {
			word = append(word, c)
		}
	}

	if !IsSpace(cmd[len(cmd)-1]) {
		result = append(result, string(word))
	}
	return result
}

func HasDirPrefix(path, prefix string) bool {
	prefix = strings.TrimRight(prefix, string(filepath.Separator))
	path = strings.TrimRight(path, string(filepath.Separator))
	return path == prefix ||
		strings.HasPrefix(path, prefix + string(filepath.Separator))
}

func EncodeFileInfo(fi os.FileInfo) string {
	fi.Atime_ns = 0
	fi.Ino = 0
	fi.Dev = 0
	fi.Name = ""
	return fmt.Sprintf("%v", fi)
}
