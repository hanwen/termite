package rpcfs

import (
	"fmt"
	"log"
	"github.com/hanwen/go-fuse/fuse"
	)
var _ = fmt.Println

// ReadOnlyFile is for implementing read-only filesystems.  This
// assumes we already have the data in memory.
type ChunkedFile struct {
	chunks [][]byte
	chunkSize uint32

	fuse.DefaultFile
}

func NewChunkedFile(data [][]byte) *ChunkedFile {
	f := new(ChunkedFile)
	f.chunks = data

	if len(data) > 0 {
		f.chunkSize = uint32(len(data[0]))
	}

	for i, v := range data {
		if i < len(data)-1 && uint32(len(v)) != f.chunkSize {
			log.Fatal("all chunks should be equal")
		}
	}
	return f
}

func (me *ChunkedFile) Read(input *fuse.ReadIn, bp fuse.BufferPool) ([]byte, fuse.Status) {
	if me.chunkSize == 0 {
		return []byte{}, fuse.OK
	}

	out := bp.AllocBuffer(input.Size)[:0]

	i := int(input.Offset / uint64(me.chunkSize))
	off := uint32(input.Offset % uint64(me.chunkSize))

	if off + input.Size < me.chunkSize {
		end := off + input.Size
		if end > uint32(len(me.chunks[i])) {
			end = uint32(len(me.chunks[i]))
		}
		return me.chunks[i][off:end], fuse.OK
	}

	for ; uint32(len(out)) < input.Size && i < len(me.chunks); i++ {
		end := len(me.chunks[i])
		if end - int(off) > (int(input.Size) - len(out)) {
			end = int(off) + int(input.Size) - len(out)
		}
		oldLen := len(out)
		out = out[:oldLen + end - int(off)]
		copy(out[oldLen:], me.chunks[i][off:end])
		off = 0
	}

	return out, fuse.OK
}
