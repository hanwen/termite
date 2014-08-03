package termite

import (
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/hanwen/go-fuse/fuse"
	"github.com/hanwen/go-fuse/fuse/nodefs"
	"github.com/hanwen/go-fuse/fuse/pathfs"
	"github.com/hanwen/termite/attr"
	termitefs "github.com/hanwen/termite/fs"
)

type workerFS struct {
	rwDir   string
	tmpDir  string
	rootDir string // absolute path of the root of the RPC backed miror FS

	// without leading /
	writableRoot string
	unionFs      *termitefs.MemUnionFs
	procFs       *termitefs.ProcFs
	rpcNodeFs    *pathfs.PathNodeFs
	unionNodeFs  *pathfs.PathNodeFs

	// Protected by Mirror.fsMutex
	id      string
	reaping bool

	// When this reaches zero, we reap the filesystem.
	tasks map[*WorkerTask]bool

	// Task ids that have results pending in this FS.
	taskIds []int
}

type fuseFS struct {
	tmpDir      string
	mount       string
	root        nodefs.Node
	server      *fuse.Server
	fsConnector *nodefs.FileSystemConnector
}

func (fs *fuseFS) Stop() {
	if err := fs.server.Unmount(); err != nil {
		// If the unmount fails, the RemoveAll will stat all
		// of the FUSE file system, so we have to exit.
		log.Panic("unmount fail in workerFS.Stop:", err)
	}
	os.RemoveAll(fs.tmpDir)
}

func (fs *fuseFS) SetDebug(debug bool) {
	fs.server.SetDebug(debug)
	fs.fsConnector.SetDebug(debug)
}

func (fs *workerFS) Status() (s FuseFsStatus) {
	s.Id = fs.id
	for t := range fs.tasks {
		s.Tasks = append(s.Tasks, t.taskInfo)
	}
	return s
}

func (fs *workerFS) addTask(task *WorkerTask) {
	fs.taskIds = append(fs.taskIds, task.req.TaskId)
	fs.tasks[task] = true
}

func (fs *workerFS) SetDebug(debug bool) {
	fs.rpcNodeFs.SetDebug(debug)
}

func newFuseFS(tmpDir string) (*fuseFS, error) {
	tmpDir, err := ioutil.TempDir(tmpDir, "termite-task")
	if err != nil {
		return nil, err
	}

	fs := &fuseFS{
		root:   nodefs.NewDefaultNode(),
		tmpDir: tmpDir,
		mount:  filepath.Join(tmpDir, "mnt"),
	}
	if err := os.Mkdir(fs.mount, 0755); err != nil {
		return nil, err
	}

	fs.fsConnector = nodefs.NewFileSystemConnector(fs.root, nil)
	fuseOpts := fuse.MountOptions{}
	if os.Geteuid() == 0 {
		fuseOpts.AllowOther = true
	}

	fs.server, err = fuse.NewServer(fs.fsConnector.RawFS(), fs.mount, &fuseOpts)
	if err != nil {
		return nil, err
	}
	go fs.server.Serve()
	return fs, nil
}

func (fuseFS *fuseFS) newWorkerFuseFs(rpcFs pathfs.FileSystem, writableRoot string, nobody *User) (*workerFS, error) {
	fs := &workerFS{
		tmpDir:       fuseFS.tmpDir,
		writableRoot: strings.TrimLeft(writableRoot, "/"),
		tasks:        map[*WorkerTask]bool{},
	}

	type dirInit struct {
		dst *string
		val string
	}

	tmpBacking := ""
	for _, v := range []dirInit{
		{&fs.rwDir, "rw"},
		{&tmpBacking, "tmp-backing"},
	} {
		*v.dst = filepath.Join(fs.tmpDir, v.val)
		if err := os.Mkdir(*v.dst, 0700); err != nil {
			return nil, err
		}
	}

	subDir := fmt.Sprintf("sub%x", rand.Int31())
	fs.rpcNodeFs = pathfs.NewPathNodeFs(rpcFs, nil)
	ttl := 30 * time.Second
	mOpts := nodefs.Options{
		EntryTimeout:    ttl,
		AttrTimeout:     ttl,
		NegativeTimeout: ttl,

		// 32-bit programs have trouble with 64-bit inode
		// numbers.
		PortableInodes: true,
	}

	if status := fuseFS.fsConnector.Mount(fuseFS.root.Inode(), subDir, fs.rpcNodeFs.Root(), &mOpts); !status.Ok() {
		return nil, fmt.Errorf("Mount root on %q: %v", subDir, status)
	}

	var err error
	fs.unionFs, err = termitefs.NewMemUnionFs(
		fs.rwDir, pathfs.NewPrefixFileSystem(rpcFs, fs.writableRoot))
	if err != nil {
		return nil, err
	}

	fs.rootDir = filepath.Join(fuseFS.mount, subDir)
	procFs := termitefs.NewProcFs()
	procFs.StripPrefix = fs.rootDir
	if nobody != nil {
		procFs.Uid = nobody.Uid
	}
	type submount struct {
		mountpoint string
		root       nodefs.Node
	}

	mounts := []submount{
		{"proc", pathfs.NewPathNodeFs(procFs, nil).Root()},
		{"sys", pathfs.NewPathNodeFs(pathfs.NewReadonlyFileSystem(pathfs.NewLoopbackFileSystem("/sys")), nil).Root()},
		{"tmp", nodefs.NewMemNodeFSRoot(tmpBacking + "/tmp")},
		{"dev", termitefs.NewDevFSRoot()},
		{"var/tmp", nodefs.NewMemNodeFSRoot(tmpBacking + "/vartmp")},
	}
	for _, s := range mounts {
		subOpts := &mOpts
		if s.mountpoint == "proc" {
			subOpts = nil
		}

		code := fs.rpcNodeFs.Mount(s.mountpoint, s.root, subOpts)
		if !code.Ok() {
			return nil, errors.New(fmt.Sprintf("submount error for %q: %v", s.mountpoint, code))
		}
	}
	if strings.HasPrefix(fs.writableRoot, "tmp/") {
		parent, _ := filepath.Split(fs.writableRoot)
		dir := filepath.Join(fuseFS.mount, subDir, parent)
		if err := os.MkdirAll(dir, 0755); err != nil {
			return nil, errors.New(fmt.Sprintf("Mkdir of %q in /tmp fail: %v", parent, err))
		}
		// This is hackish, but we don't want rpcfs/fsserver
		// getting confused by asking for tmp/foo/bar
		// directly.
		rpcFs.GetAttr("tmp", nil)
		rpcFs.GetAttr(fs.writableRoot, nil)
	}
	code := fs.rpcNodeFs.Mount(fs.writableRoot, fs.unionFs.Root(), &mOpts)
	if !code.Ok() {
		return nil, errors.New(fmt.Sprintf("submount writable root %s: %v", fs.writableRoot, code))
	}

	return fs, nil
}

func (fs *workerFS) update(attrs []*attr.FileAttr) {
	updates := map[string]*termitefs.Result{}
	for _, attr := range attrs {
		path := strings.TrimLeft(attr.Path, "/")
		if !strings.HasPrefix(path, fs.writableRoot) {
			fs.rpcNodeFs.Notify(path)
			continue
		}
		path = strings.TrimLeft(path[len(fs.writableRoot):], "/")

		if attr.Deletion() {
			updates[path] = &termitefs.Result{}
		} else {
			r := termitefs.Result{
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
	fs.unionFs.Update(updates)
}

func (fs *workerFS) reap() (dir string, yield map[string]*termitefs.Result) {
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
