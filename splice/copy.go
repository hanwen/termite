package splice

import (
	"io"
	"os"
	"syscall"
)

func SpliceCopy(dst *os.File, src *os.File, p *splicePair) (int64, error) {
	total := int64(0)

	for {
		n, err := syscall.Splice(src.Fd(), nil, p.w.Fd(), nil, p.size, 0)
		if err != nil {
			return total, os.NewSyscallError("Splice", err)
		}
		if n == 0 {
			break
		}
		m, err := syscall.Splice(p.r.Fd(), nil, dst.Fd(), nil, int(n), 0)
		if err != nil {
			return total, os.NewSyscallError("Splice", err)
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
func CopyFile(dstName string, srcName string, mode int) error {
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

func CopyFds(dst *os.File, src *os.File) (err error) {
	p, err := splicePool.get()
	if p != nil {
		p.Grow(256 * 1024)
		_, err := SpliceCopy(dst, src, p)
		splicePool.done(p)
		return err
	} else {
		_, err = io.Copy(dst, src)
	}
	if err == io.EOF {
		err = nil
	}
	return err
}
