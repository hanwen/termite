package termite

import (
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/hanwen/go-fuse/fuse"
	"github.com/hanwen/go-fuse/fuse/nodefs"
	"github.com/hanwen/go-fuse/fuse/pathfs"
	"github.com/hanwen/termite/attr"
	termitefs "github.com/hanwen/termite/fs"
)

type workerFSState struct {
	// Protected by Mirror.fsMutex
	reaping bool

	// When this reaches zero, we reap the filesystem.
	tasks map[*WorkerTask]bool

	// Task ids that have results pending in this FS.
	taskIds []int

	// workerFS that this state belongs to.
	fs *workerFS
}

func newWorkerFSState(fs *workerFS) *workerFSState {
	return &workerFSState{
		tasks: map[*WorkerTask]bool{},
		fs:    fs,
	}
}

type workerFS struct {
	fuseFS  *fuseFS
	id      string
	rwDir   string
	tmpDir  string
	rootDir string // absolute path of the root of the RPC backed miror FS

	// without leading /
	unionFs     *termitefs.MemUnionFs
	unionNodeFs *pathfs.PathNodeFs

	state *workerFSState
}

type fuseFS struct {
	tmpDir       string
	mount        string
	root         nodefs.Node
	server       *fuse.Server
	fsConnector  *nodefs.FileSystemConnector
	rpcNodeFS    *pathfs.PathNodeFs
	rpcFS        *RpcFs
	writableRoot string
	nobody       *User

	// indexed by prefix inside the root
	workerMu sync.Mutex
	workers  map[string]*workerFS
}

type multiRPCFS struct {
	*RpcFs
}

var workerRegex = regexp.MustCompile("^worker[0-9]{4}/?")

const prefixLen = len("worker1234")

func (m *multiRPCFS) strip(n string) string {
	if workerRegex.MatchString(n) {
		if len(n) == prefixLen {
			return ""
		}
		return n[prefixLen+1:]
	}
	return n
}

func (fs *multiRPCFS) OpenDir(name string, context *fuse.Context) ([]fuse.DirEntry, fuse.Status) {
	return fs.RpcFs.OpenDir(fs.strip(name), context)
}

func (fs *multiRPCFS) Open(name string, flags uint32, context *fuse.Context) (nodefs.File, fuse.Status) {
	return fs.RpcFs.Open(fs.strip(name), flags, context)
}

func (fs *multiRPCFS) Readlink(name string, context *fuse.Context) (string, fuse.Status) {
	return fs.RpcFs.Readlink(fs.strip(name), context)
}

func (fs *multiRPCFS) GetAttr(name string, context *fuse.Context) (*fuse.Attr, fuse.Status) {
	return fs.RpcFs.GetAttr(fs.strip(name), context)
}

func (fs *multiRPCFS) Access(name string, mode uint32, context *fuse.Context) (code fuse.Status) {
	return fs.RpcFs.Access(fs.strip(name), mode, context)
}

func (fs *fuseFS) addWorkerFS() (*workerFS, error) {
	fs.workerMu.Lock()
	defer fs.workerMu.Unlock()
	id := fmt.Sprintf("worker%04d", len(fs.workers))

	wfs, err := fs.newWorkerFS(id)
	if err != nil {
		return nil, err
	}
	fs.workers[id] = wfs
	return wfs, nil
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

func (state *workerFSState) Status() (s FuseFsStatus) {
	s.Id = state.fs.id
	for t := range state.tasks {
		s.Tasks = append(s.Tasks, t.taskInfo)
	}
	return s
}

func (fs *workerFSState) addTask(task *WorkerTask) {
	fs.taskIds = append(fs.taskIds, task.req.TaskId)
	fs.tasks[task] = true
}

func (fs *workerFS) SetDebug(debug bool) {
	fs.fuseFS.rpcNodeFS.SetDebug(debug)
}

func nodeFSOptions() *nodefs.Options {
	ttl := 30 * time.Second
	return &nodefs.Options{
		EntryTimeout:    ttl,
		AttrTimeout:     ttl,
		NegativeTimeout: ttl,

		// 32-bit programs have trouble with 64-bit inode
		// numbers.
		PortableInodes: true,
	}
}

func newFuseFS(tmpDir string, rpcFS *RpcFs, writableRoot string) (*fuseFS, error) {
	tmpDir, err := ioutil.TempDir(tmpDir, "termite-task")
	if err != nil {
		return nil, err
	}

	fs := &fuseFS{
		writableRoot: strings.TrimLeft(writableRoot, "/"),
		workers:      map[string]*workerFS{},
		rpcFS:        rpcFS,
		rpcNodeFS: pathfs.NewPathNodeFs(&multiRPCFS{rpcFS},
			&pathfs.PathNodeFsOptions{ClientInodes: true}),
		tmpDir: tmpDir,
		mount:  filepath.Join(tmpDir, "mnt"),
	}
	if err := os.Mkdir(fs.mount, 0755); err != nil {
		return nil, err
	}

	fs.fsConnector = nodefs.NewFileSystemConnector(fs.rpcNodeFS.Root(),
		nodeFSOptions())
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

func (fuseFS *fuseFS) newWorkerFS(id string) (*workerFS, error) {
	fs := &workerFS{
		id:     id,
		fuseFS: fuseFS,
		tmpDir: filepath.Join(fuseFS.tmpDir, id),
	}
	fs.state = newWorkerFSState(fs)

	type dirInit struct {
		dst *string
		val string
	}

	fs.rwDir = filepath.Join(fs.tmpDir, "rw")
	if err := os.MkdirAll(fs.rwDir, 0700); err != nil {
		return nil, err
	}

	var err error
	fs.unionFs, err = termitefs.NewMemUnionFs(
		fs.rwDir, pathfs.NewPrefixFileSystem(fuseFS.rpcFS, fs.fuseFS.writableRoot))
	if err != nil {
		return nil, err
	}

	fs.rootDir = filepath.Join(fuseFS.mount, id)
	if code := fs.fuseFS.rpcNodeFS.Mount(
		id, fs.unionFs.Root(), nodeFSOptions()); !code.Ok() {
		return nil, errors.New(fmt.Sprintf("submount writable root %s: %v", fs.fuseFS.writableRoot, code))
	}
	return fs, nil
}

func (fs *workerFS) update(attrs []*attr.FileAttr) {
	updates := map[string]*termitefs.Result{}
	for _, attr := range attrs {
		path := strings.TrimLeft(attr.Path, "/")
		if !strings.HasPrefix(path, fs.fuseFS.writableRoot) {
			dir, name := filepath.Split(path)

			// As file contents are immutable, we must
			// invalidate the entry instead
			fs.fuseFS.rpcNodeFS.EntryNotify(filepath.Join(fs.id, dir), name)
			continue
		}
		path = strings.TrimLeft(path[len(fs.fuseFS.writableRoot):], "/")

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
