package termite

import (
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"syscall"
)

var _ = log.Println

type splicePair struct {
	r, w *os.File
	size int
}

var splicePairs chan *splicePair

var pipeMaxSize *int

const defaultPipeSize = 16 * 4096

func init() {
	splicePairs = make(chan *splicePair, 100)
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
func fcntl(fd int, cmd int, arg int) (val int, errno int) {
	r0, _, e1 := syscall.Syscall(syscall.SYS_FCNTL, uintptr(fd), uintptr(cmd), uintptr(arg))
	val = int(r0)
	errno = int(e1)
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

func (me *splicePair) Close() os.Error {
	err1 := me.r.Close()
	err2 := me.w.Close()
	if err1 != nil {
		return err1
	}
	return err2
}

func newSplicePair() (me *splicePair, err os.Error) {
	me = &splicePair{}
	me.r, me.w, err = os.Pipe()
	if err != nil {
		return nil, err
	}

	errNo := 0
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
		// From manpage on ubuntu Lucid:
		//
		// Since Linux 2.6.11, the pipe capacity is 65536 bytes.
		me.size = defaultPipeSize
		return me, nil
	}
	if errNo != 0 {
		me.Close()
		return nil, os.NewSyscallError("fcntl getsize", errNo)
	}
	return me, nil
}

func getSplice() (p *splicePair, err os.Error) {
	select {
	case p = <-splicePairs:
		// already done.
	default:
		p, err = newSplicePair()
	}
	return p, err
}

func returnSplice(p *splicePair) {
	if p != nil {
		splicePairs <- p
	}
}

func SpliceCopy(dst *os.File, src *os.File, p *splicePair) (int64, os.Error) {
	total := int64(0)

	for {
		n, errNo := syscall.Splice(src.Fd(), nil, p.w.Fd(), nil, p.size, 0)
		if errNo != 0 {
			return total, os.NewSyscallError("Splice", errNo)
		}
		if n == 0 {
			break
		}
		m, errNo := syscall.Splice(p.r.Fd(), nil, dst.Fd(), nil, int(n), 0)
		if errNo != 0 {
			return total, os.NewSyscallError("Splice", errNo)
		}
		if m < n {
			panic("m<n")
		}
		total += int64(m)
		if int(n) < p.size {
			break
		}
	}

	return total, nil
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

	return CopyFds(dst, src)
}

func CopyFds(dst *os.File, src *os.File) (err os.Error) {
	p, err := getSplice()
	if p != nil {
		p.Grow(256 * 1024)
		_, err := SpliceCopy(dst, src, p)
		returnSplice(p)
		return err
	} else {
		_, err = io.Copy(dst, src)
	}
	if err == os.EOF {
		err = nil
	}
	return err
}
