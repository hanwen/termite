package fs

import (
	"os"

	"github.com/hanwen/go-fuse/fuse"
	"github.com/hanwen/go-fuse/fuse/nodefs"
)

const _NULL = "null"

type devFSRoot struct {
	nodefs.Node
}

func NewDevFSRoot() nodefs.Node {
	r := &devFSRoot{
		nodefs.NewDefaultNode(),
	}
	return r
}

func (r *devFSRoot) OnMount(fsc *nodefs.FileSystemConnector) {
	def := nodefs.NewDefaultNode()
	r.Inode().NewChild("null", false, &nullNode{Node: def})
	r.Inode().NewChild("urandom", false, &urandomNode{Node: def, size: 128})
}

type nullNode struct {
	nodefs.Node
}

func (me *nullNode) Deletable() bool {
	return false
}

func (me *nullNode) Access(mode uint32, context *fuse.Context) (code fuse.Status) {
	return fuse.OK
}

func (me *nullNode) Truncate(file nodefs.File, offset uint64, context *fuse.Context) (code fuse.Status) {
	return fuse.OK
}

func (me *nullNode) Open(flags uint32, context *fuse.Context) (file nodefs.File, code fuse.Status) {
	return nodefs.NewDevNullFile(), fuse.OK
}

type urandomNode struct {
	nodefs.Node
	size int
}

func (me *urandomNode) Deletable() bool {
	return false
}

func (me *urandomNode) GetAttr(out *fuse.Attr, file nodefs.File, context *fuse.Context) (code fuse.Status) {
	out.Mode = uint32(fuse.S_IFREG | 0444)
	out.Size = uint64(me.size)
	return fuse.OK
}

func (me *urandomNode) Access(mode uint32, context *fuse.Context) (code fuse.Status) {
	return fuse.OK
}

func (me *urandomNode) Open(flags uint32, context *fuse.Context) (file nodefs.File, code fuse.Status) {
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

	return nodefs.NewDataFile(randData[:n]), fuse.OK
}
