package splice

import (
	"fmt"
	"log"
	"os"
	"syscall"
)

type Pair struct {
	r, w *os.File
	size int
}

func (p *Pair) MaxGrow() {
	for p.Grow(2 * p.size) {
	}
}

func (p *Pair) Grow(n int) bool {
	if n > getPipeMaxSize() {
		return false
	}
	if n <= p.size {
		return true
	}
	newsize := p.size
	for newsize < n {
		newsize *= 2
	}

	newsize, errNo := fcntl(p.r.Fd(), F_SETPIPE_SZ, newsize)
	if errNo != 0 {
		log.Println(os.NewSyscallError("fnct", errNo))
		return false
	}
	p.size = newsize
	return true
}

func (p *Pair) Cap() int {
	return p.size
}

func (p *Pair) Close() error {
	err1 := p.r.Close()
	err2 := p.w.Close()
	if err1 != nil {
		return err1
	}
	return err2
}

func (p *Pair) Read(d []byte) (n int, err error) {
	return p.r.Read(d)
}

func (p *Pair) LoadFrom(fd uintptr, sz int) (int, error) {
	if sz > p.size {
		return 0, fmt.Errorf("LoadFrom: not enough space %d, %d",
			sz, p.size)
	}

	n, err := syscall.Splice(int(fd), nil, int(p.w.Fd()), nil, sz, 0)
	if err != nil {
		err = os.NewSyscallError("Splice load from", err)
	}
	return int(n), err
}

func (p *Pair) WriteTo(fd uintptr, n int) (int, error) {
	m, err := syscall.Splice(int(p.r.Fd()), nil, int(fd), nil, int(n), 0)
	if err != nil {
		err = os.NewSyscallError("Splice write to: ", err)
	}
	return int(m), err
}
