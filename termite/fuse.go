package termite

import (
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/hanwen/go-fuse/fuse"
	"github.com/hanwen/go-fuse/fuse/nodefs"
	"github.com/hanwen/go-fuse/fuse/pathfs"
	"github.com/hanwen/termite/attr"
	"github.com/hanwen/termite/fs"
)

type workerFuseFs struct {
	rwDir  string
	tmpDir string
	mount  string

	// without leading /
	writableRoot string
	*fuse.Server
	fsConnector *nodefs.FileSystemConnector
	unionFs     *fs.MemUnionFs
	procFs      *fs.ProcFs
	rpcNodeFs   *pathfs.PathNodeFs
	unionNodeFs *pathfs.PathNodeFs

	// Protected by Mirror.fsMutex
	id      string
	reaping bool

	// When this reaches zero, we reap the filesystem.
	tasks map[*WorkerTask]bool

	// Task ids that have results pending in this FS.
	taskIds []int
}

func (me *workerFuseFs) Status() (s FuseFsStatus) {
	s.Id = me.id
	s.Mem = me.DebugData()
	for t := range me.tasks {
		s.Tasks = append(s.Tasks, t.taskInfo)
	}
	return s
}

func (me *workerFuseFs) addTask(task *WorkerTask) {
	me.taskIds = append(me.taskIds, task.req.TaskId)
	me.tasks[task] = true
}

func (me *workerFuseFs) Stop() {
	if err := me.Server.Unmount(); err != nil {
		// If the unmount fails, the RemoveAll will stat all
		// of the FUSE file system, so we have to exit.
		log.Panic("unmount fail in workerFuseFs.Stop:", err)
	}

	os.RemoveAll(me.tmpDir)
}

func (me *workerFuseFs) SetDebug(debug bool) {
	me.Server.SetDebug(debug)
	me.fsConnector.SetDebug(debug)
	me.rpcNodeFs.SetDebug(debug)
}

func newWorkerFuseFs(tmpDir string, rpcFs pathfs.FileSystem, writableRoot string, nobody *User) (*workerFuseFs, error) {
	tmpDir, err := ioutil.TempDir(tmpDir, "termite-task")
	if err != nil {
		return nil, err
	}
	me := &workerFuseFs{
		tmpDir:       tmpDir,
		writableRoot: strings.TrimLeft(writableRoot, "/"),
		tasks:        map[*WorkerTask]bool{},
	}

	type dirInit struct {
		dst *string
		val string
	}

	tmpBacking := ""
	for _, v := range []dirInit{
		{&me.rwDir, "rw"},
		{&me.mount, "mnt"},
		{&tmpBacking, "tmp-backing"},
	} {
		*v.dst = filepath.Join(me.tmpDir, v.val)
		err = os.Mkdir(*v.dst, 0700)
		if err != nil {
			return nil, err
		}
	}

	fuseOpts := fuse.MountOptions{}
	if os.Geteuid() == 0 {
		fuseOpts.AllowOther = true
	}

	me.rpcNodeFs = pathfs.NewPathNodeFs(rpcFs, nil)
	ttl := 30 * time.Second
	mOpts := nodefs.Options{
		EntryTimeout:    ttl,
		AttrTimeout:     ttl,
		NegativeTimeout: ttl,

		// 32-bit programs have trouble with 64-bit inode
		// numbers.
		PortableInodes: true,
	}

	me.fsConnector = nodefs.NewFileSystemConnector(me.rpcNodeFs.Root(), &mOpts)
	me.Server, err = fuse.NewServer(me.fsConnector.RawFS(), me.mount, &fuseOpts)
	if err != nil {
		return nil, err
	}
	go me.Server.Serve()

	me.unionFs, err = fs.NewMemUnionFs(
		me.rwDir, pathfs.NewPrefixFileSystem(rpcFs, me.writableRoot))
	if err != nil {
		return nil, err
	}

	me.procFs = fs.NewProcFs()
	me.procFs.StripPrefix = me.mount
	if nobody != nil {
		me.procFs.Uid = nobody.Uid
	}
	type submount struct {
		mountpoint string
		root         nodefs.Node
	}

	mounts := []submount{
		{"proc", pathfs.NewPathNodeFs(me.procFs, nil).Root()},
		{"sys", pathfs.NewPathNodeFs(pathfs.NewReadonlyFileSystem(pathfs.NewLoopbackFileSystem("/sys")), nil).Root()},
		{"tmp", nodefs.NewMemNodeFSRoot(tmpBacking + "/tmp")},
		{"dev", fs.NewDevFSRoot()},
		{"var/tmp", nodefs.NewMemNodeFSRoot(tmpBacking + "/vartmp")},
	}
	for _, s := range mounts {
		subOpts := &mOpts
		if s.mountpoint == "proc" {
			subOpts = nil
		}

		code := me.rpcNodeFs.Mount(s.mountpoint, s.root, subOpts)
		if !code.Ok() {
			if err := me.Server.Unmount(); err != nil {
				log.Fatal("FUSE unmount error during cleanup:", err)
			}
			return nil, errors.New(fmt.Sprintf("submount error for %s: %v", s.mountpoint, code))
		}
	}
	if strings.HasPrefix(me.writableRoot, "tmp/") {
		parent, _ := filepath.Split(me.writableRoot)
		err := os.MkdirAll(filepath.Join(me.mount, parent), 0755)
		if err != nil {
			if err := me.Server.Unmount(); err != nil {
				log.Fatal("FUSE unmount error during cleanup:", err)
			}
			return nil, errors.New(fmt.Sprintf("Mkdir of %q in /tmp fail: %v", parent, err))
		}
		// This is hackish, but we don't want rpcfs/fsserver
		// getting confused by asking for tmp/foo/bar
		// directly.
		rpcFs.GetAttr("tmp", nil)
		rpcFs.GetAttr(me.writableRoot, nil)
	}
	code := me.rpcNodeFs.Mount(me.writableRoot, me.unionFs.Root(), &mOpts)
	if !code.Ok() {
		if err := me.Server.Unmount(); err != nil {
			log.Fatal("FUSE unmount error during cleanup:", err)
		}
		return nil, errors.New(fmt.Sprintf("submount error for %s: %v", me.writableRoot, code))
	}

	return me, nil
}

func (me *workerFuseFs) update(attrs []*attr.FileAttr) {
	updates := map[string]*fs.Result{}
	for _, attr := range attrs {
		path := strings.TrimLeft(attr.Path, "/")
		if !strings.HasPrefix(path, me.writableRoot) {
			me.rpcNodeFs.Notify(path)
			continue
		}
		path = strings.TrimLeft(path[len(me.writableRoot):], "/")

		if attr.Deletion() {
			updates[path] = &fs.Result{}
		} else {
			r := fs.Result{
				Original: "",
				Backing:  "",
				Link:     attr.Link,
				Attr:     &fuse.Attr{},
			}
			a := *attr.Attr
			r.Attr = &a
			updates[path] = &r
		}
	}
	me.unionFs.Update(updates)
}

func (fs *workerFuseFs) reap() (dir string, yield map[string]*fs.Result) {
	yield = fs.unionFs.Reap()
	backingStoreFiles := map[string]string{}
	dir, err := ioutil.TempDir(fs.tmpDir, "reap")
	if err != nil {
		log.Fatalf("ioutil.TempDir: %v", err)
	}

	i := 0
	for _, v := range yield {
		if v.Backing == "" {
			continue
		}
		newBacking := backingStoreFiles[v.Backing]
		if newBacking == "" {
			newBacking = fmt.Sprintf("%s/%d", dir, i)
			i++

			err := os.Rename(v.Backing, newBacking)
			if err != nil {
				log.Panicf("reapFiles rename failed: %v", err)
			}
			log.Printf("created %q", newBacking)
			backingStoreFiles[v.Backing] = newBacking
		}
		v.Backing = newBacking
	}

	// We saved the backing store files, so we don't need the file system anymore.
	fs.unionFs.Reset()
	return dir, yield
}
