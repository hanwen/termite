package cba

import (
	"hash"
	"io"
	"log"
	"os"
	"path/filepath"
	"time"
)

type HashWriter struct {
	start  time.Time
	hasher hash.Hash
	dest   *os.File
	cache  *Store
	size   int
}

func (st *HashWriter) Sum() string {
	return string(st.hasher.Sum(nil))
}

func (st *HashWriter) Write(p []byte) (n int, err error) {
	n, err = st.dest.Write(p)
	st.hasher.Write(p[:n])
	st.size += n
	return n, err
}

func (st *HashWriter) WriteClose(p []byte) (err error) {
	_, err = st.Write(p)
	if err != nil {
		return err
	}
	err = st.Close()
	return err
}

func (st *HashWriter) CopyClose(input io.Reader, size int64) error {
	_, err := io.CopyN(st, input, size)
	if err != nil {
		return err
	}
	err = st.Close()
	return err
}

func (st *HashWriter) Close() error {
	st.dest.Chmod(0444)
	err := st.dest.Close()
	if err != nil {
		return err
	}
	src := st.dest.Name()
	dir, _ := filepath.Split(src)
	sum := st.Sum()
	sumpath := HashPath(dir, sum)

	log.Printf("saving hash %x\n", sum)
	err = os.Rename(src, sumpath)
	if err != nil {
		log.Fatal("Rename failed", err)
	}

	dt := time.Now().Sub(st.start)

	st.cache.AddTiming("Save", st.size, dt)

	return err
}
