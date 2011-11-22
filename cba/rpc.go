package cba
import (
	"fmt"
	"github.com/hanwen/go-fuse/fuse"
)

type ContentRequest struct {
       Hash       string
       Start, End int
}

func (me *ContentRequest) String() string {
       return fmt.Sprintf("%x [%d, %d]", me.Hash, me.Start, me.End)
}

type ContentResponse struct {
       Chunk []byte
}

type OsFileInfo fuse.OsFileInfo
type OsFileInfos fuse.OsFileInfos

