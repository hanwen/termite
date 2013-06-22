package fs

import (
	"os"

	"github.com/hanwen/go-fuse/fuse"
)

const _NULL = "null"

type DevFs struct {
	fuse.DefaultNodeFileSystem
	root fuse.DefaultFsNode
}

func NewDevFs() *DevFs {
	me := &DevFs{}
	return me
}

func (me *DevFs) OnMount(fsc *fuse.FileSystemConnector) {
	n := me.root.Inode().New(false, &nullNode{})
	me.root.Inode().AddChild("null", n)
	n = me.root.Inode().New(false, &urandomNode{size: 128})
	me.root.Inode().AddChild("urandom", n)
}

func (me *DevFs) Root() fuse.FsNode {
	return &me.root
}

func (me *DevFs) String() string {
	return "DevFs"
}

type nullNode struct {
	fuse.DefaultFsNode
}

func (me *nullNode) Deletable() bool {
	return false
}

func (me *nullNode) Access(mode uint32, context *fuse.Context) (code fuse.Status) {
	return fuse.OK
}

func (me *nullNode) Truncate(file fuse.File, offset uint64, context *fuse.Context) (code fuse.Status) {
	return fuse.OK
}

func (me *nullNode) Open(flags uint32, context *fuse.Context) (file fuse.File, code fuse.Status) {
	return fuse.NewDevNullFile(), fuse.OK
}

type urandomNode struct {
	fuse.DefaultFsNode
	size int
}

func (me *urandomNode) Deletable() bool {
	return false
}

func (me *urandomNode) GetAttr(out *fuse.Attr, file fuse.File, context *fuse.Context) (code fuse.Status) {
	out.Mode = uint32(fuse.S_IFREG | 0444)
	out.Size = uint64(me.size)
	return fuse.OK
}

func (me *urandomNode) Access(mode uint32, context *fuse.Context) (code fuse.Status) {
	return fuse.OK
}

func (me *urandomNode) Open(flags uint32, context *fuse.Context) (file fuse.File, code fuse.Status) {
	if flags&fuse.O_ANYWRITE != 0 {
		return nil, fuse.EPERM
	}
	f, err := os.Open("/dev/urandom")
	if err != nil {
		return nil, fuse.ToStatus(err)
	}
	defer f.Close()

	randData := make([]byte, me.size)
	n, err := f.Read(randData)
	if err != nil {
		return nil, fuse.ToStatus(err)
	}

	return fuse.NewDataFile(randData[:n]), fuse.OK
}
