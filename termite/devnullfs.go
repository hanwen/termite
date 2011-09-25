package termite

import (
	"github.com/hanwen/go-fuse/fuse"
)

const _NULL = "null"

type DevNullFs struct {
	fuse.DefaultNodeFileSystem
	root fuse.DefaultFsNode
}

func NewDevNullFs() *DevNullFs {
	me := &DevNullFs{}
	return me
}

func (me *DevNullFs) OnMount(fsc *fuse.FileSystemConnector) {
	n := me.root.Inode().New(false, &devNullNode{})
	me.root.Inode().AddChild("null", n)
}

func (me *DevNullFs) Root() fuse.FsNode {
	return &me.root
}

type devNullNode struct {
	fuse.DefaultFsNode
}

func (me *devNullNode) Deletable() bool {
	return false
}

func (me *devNullNode) Access(mode uint32, context *fuse.Context) (code fuse.Status) {
	return fuse.OK
}

func (me *devNullNode) Truncate(file fuse.File, offset uint64, context *fuse.Context) (code fuse.Status) {
	return fuse.OK
}

func (me *devNullNode) Open(flags uint32, context *fuse.Context) (file fuse.File, code fuse.Status) {
	return fuse.NewDevNullFile(), fuse.OK
}
