package termite

import (
	"crypto"
	"fmt"
	"github.com/hanwen/go-fuse/fuse"
	"io"
	"log"
	"os"
	"rand"
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
