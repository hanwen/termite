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

type splicePair struct {
	r, w *os.File
	size int
}

type splicePairPool struct {
	sync.Mutex
	unused map[*splicePair]bool
}

func ClearSplicePool() {
	splicePool.clear()
}

func newSplicePairPool() *splicePairPool {
	return &splicePairPool{
		unused: make(map[*splicePair]bool),
	}
}

func (me *splicePairPool) clear() {
	me.Lock()
	defer me.Unlock()
	for p := range me.unused {
		p.Close()
	}
	me.unused = make(map[*splicePair]bool)
}

func (me *splicePairPool) get() (p *splicePair, err error) {
	me.Lock()
	defer me.Unlock()

	for s := range me.unused {
		delete(me.unused, s)
		return s, nil
	}

	return newSplicePair()
}

func (me *splicePairPool) done(p *splicePair) {
	me.Lock()
	defer me.Unlock()

	me.unused[p] = true
}

var splicePool *splicePairPool
var pipeMaxSize *int

// From manpage on ubuntu Lucid:
//
// Since Linux 2.6.11, the pipe capacity is 65536 bytes.
const defaultPipeSize = 16 * 4096

func init() {
	splicePool = newSplicePairPool()
}

func getPipeMaxSize() int {
	if pipeMaxSize != nil {
		return *pipeMaxSize
	}
	content, err := ioutil.ReadFile("/proc/sys/fs/pipe-max-size")
	if err != nil {
		m := defaultPipeSize
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

func (me *splicePair) MaxGrow() {
	for me.Grow(2 * me.size) {
	}
}

func (me *splicePair) Grow(n int) bool {
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

func (me *splicePair) Close() error {
	err1 := me.r.Close()
	err2 := me.w.Close()
	if err1 != nil {
		return err1
	}
	return err2
}

func newSplicePair() (me *splicePair, err error) {
	me = &splicePair{}
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
		me.size = defaultPipeSize
		return me, nil
	}
	if errNo != 0 {
		me.Close()
		return nil, os.NewSyscallError("fcntl getsize", errNo)
	}
	return me, nil
}

