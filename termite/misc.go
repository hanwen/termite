package termite

import (
	"crypto"
	"log"
	"io"
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

func md5(c []byte) []byte {
	h := crypto.MD5.New()
	h.Write(c)
	return h.Sum()
}
