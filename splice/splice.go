package splice

// Routines for efficient file to file copying.

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"syscall"
)

var _ = log.Println


var pipeMaxSize *int

// From manpage on ubuntu Lucid:
//
// Since Linux 2.6.11, the pipe capacity is 65536 bytes.
const DefaultPipeSize = 16 * 4096

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

func newSplicePair() (me *Pair, err error) {
	me = &Pair{}
	me.r, me.w, err = os.Pipe()
	if err != nil {
		return nil, err
	}

	errNo := syscall.Errno(0)
	for _, f := range []*os.File{me.r, me.w} {
		_, errNo = fcntl(f.Fd(), syscall.F_SETFL, os.O_NONBLOCK)
		if errNo != 0 {
			me.Close()
			return nil, os.NewSyscallError("fcntl setfl", errNo)
		}
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

