package termite
import (
	"fmt"
	"io"
	"log"
	"os"
	"syscall"
)
var _ = log.Println
const _PIPE_SIZE = 4096
type splicePair struct {
	r, w *os.File
	size int
}
var splicePairs chan *splicePair

var pipeMaxSize = 1 << 20
func init() {
	splicePairs = make(chan *splicePair, 100)
	f, err := os.Open("/proc/sys/fs/pipe-max-size")
	if err != nil {
		return
	}
	defer f.Close()
	fmt.Fscan(f, &pipeMaxSize)
}

// copy & paste from syscall.
func fcntl(fd int, cmd int, arg int) (val int, errno int) {
        r0, _, e1 := syscall.Syscall(syscall.SYS_FCNTL, uintptr(fd), uintptr(cmd), uintptr(arg))
        val = int(r0)
        errno = int(e1)
        return
}


const F_SETPIPE_SZ = 1031


func (me *splicePair) Grow(n int) bool {
	if n > pipeMaxSize {
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

func newSplicePair() *splicePair {
	me := &splicePair{}
	var err os.Error
	me.r, me.w, err = os.Pipe()
	if err != nil {
		return nil
	}
	me.size = 16 * 4096

	_, errR := fcntl(me.r.Fd(), syscall.F_SETFL, os.O_NONBLOCK)
	_, errW := fcntl(me.w.Fd(), syscall.F_SETFL, os.O_NONBLOCK)
	if errR != 0 || errW != 0 {
		me.Close()
		return nil
	}
	return me
}

func getSplice() (p *splicePair) {
	select {
	case p = <-splicePairs:
		// already done.
        default:
		p = newSplicePair()
        }
	return newSplicePair()
}

func returnSplice(p *splicePair) {
	if p != nil {
		splicePairs <- p
	}
}

func SpliceCopy(dst *os.File, src *os.File, p *splicePair) (int, os.Error) {
	total := 0 
	
	for {
		n, errNo := syscall.Splice(src.Fd(), nil, p.w.Fd(), nil, p.size, 0)
		if errNo != 0 {
			return total, os.NewSyscallError("Splice", errNo)
		}
		if n == 0 {
			break			
		}
		m, errNo := syscall.Splice(p.r.Fd(), nil, dst.Fd(), nil, n, 0)
		if errNo != 0 {
			return total, os.NewSyscallError("Splice", errNo)
		}
		if m < n {
			panic("m<n")
		}
		total += m
		if n < p.size {
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
	p := getSplice()
	if p != nil {
		p.Grow(256*1024)
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
