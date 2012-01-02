package splice

// Routines for efficient file to file copying.

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"sync"
	"syscall"
)

var _ = log.Println

type Pair struct {
	r, w *os.File
	size int
}

type pairPool struct {
	sync.Mutex
	unused map[*Pair]bool
}


func getPipeMaxSize() int {
	if pipeMaxSize != nil {
		return *pipeMaxSize
	}
	content, err := ioutil.ReadFile("/proc/sys/fs/pipe-max-size")
	if err != nil {
		m := DefaultPipeSize
		pipeMaxSize = &m
		return m
	}
	i := 0
	pipeMaxSize = &i
	fmt.Sscan(string(content), pipeMaxSize)
	return *pipeMaxSize
}

// copy & paste from syscall.
func fcntl(fd int, cmd int, arg int) (val int, errno syscall.Errno) {
	r0, _, e1 := syscall.Syscall(syscall.SYS_FCNTL, uintptr(fd), uintptr(cmd), uintptr(arg))
	val = int(r0)
	errno = syscall.Errno(e1)
	return
}

const F_SETPIPE_SZ = 1031
const F_GETPIPE_SZ = 1032

func (me *Pair) MaxGrow() {
	for me.Grow(2 * me.size) {
	}
}

func (me *Pair) Grow(n int) bool {
	if n > getPipeMaxSize() {
		return false
	}
	if n <= me.size {
		return true
	}
	newsize := me.size
	for newsize < n {
		newsize *= 2
	}

	newsize, errNo := fcntl(me.r.Fd(), F_SETPIPE_SZ, newsize)
	if errNo != 0 {
		log.Println(os.NewSyscallError("fnct", errNo))
		return false
	}
	me.size = newsize
	return true
}

func (me *Pair) Close() error {
	err1 := me.r.Close()
	err2 := me.w.Close()
	if err1 != nil {
		return err1
	}
	return err2
}

func (p *Pair) Read(d []byte) (n int, err error) {
	return p.r.Read(d)
}

func (p *Pair) LoadFrom(fd int, sz int) (n int, err error) {
	if sz < p.size {
		return 0, fmt.Errorf("LoadFrom: not enough space %d, %d",
			sz, p.size)
	}
	
	n, err = syscall.Splice(fd, nil, p.w.Fd(), nil, sz, 0)
	if err != nil {
		err = os.NewSyscallError("Splice load from", err)
	}
	return 
}

func (p *Pair) WriteTo(fd int, n int) (m int, err error) {
	m, err = syscall.Splice(p.r.Fd(), nil, fd, nil, int(n), 0)
	if err != nil {
		err = os.NewSyscallError("Splice write to: ", err)
	}
	return
}

func newSplicePair() (me *Pair, err error) {
	me = &Pair{}
	me.r, me.w, err = os.Pipe()
	if err != nil {
		return nil, err
	}

	errNo := syscall.Errno(0)
	_, errNo = fcntl(me.r.Fd(), syscall.F_SETFL, os.O_NONBLOCK)
	if errNo != 0 {
		me.Close()
		return nil, os.NewSyscallError("fcntl setfl r", errNo)
	}
	_, errNo = fcntl(me.w.Fd(), syscall.F_SETFL, os.O_NONBLOCK)
	if errNo != 0 {
		me.Close()
		return nil, os.NewSyscallError("fcntl setfl w", errNo)
	}

	me.size, errNo = fcntl(me.r.Fd(), F_GETPIPE_SZ, 0)
	if errNo == syscall.EINVAL {
		me.size = DefaultPipeSize
		return me, nil
	}
	if errNo != 0 {
		me.Close()
		return nil, os.NewSyscallError("fcntl getsize", errNo)
	}
	return me, nil
}

