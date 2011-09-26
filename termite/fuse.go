package termite

import (
	"fmt"
	"github.com/hanwen/go-fuse/fuse"
	"github.com/hanwen/go-fuse/unionfs"
	"io/ioutil"
	"log"
	"os"
	"os/user"
	"path/filepath"
	"strings"
)

type workerFuseFs struct {
	rwDir  string
	tmpDir string
	mount  string

	// without leading /
	writableRoot string
	*fuse.MountState
	fsConnector *fuse.FileSystemConnector
	unionFs     *unionfs.UnionFs
	procFs      *ProcFs
	nodeFs      *fuse.PathNodeFs
	unionNodeFs *fuse.PathNodeFs
	// If nil, we are running this task.
	task *WorkerTask
}

func (me *workerFuseFs) Stop() {
	err := me.MountState.Unmount()
	if err != nil {
		// TODO - Should be fatal?
		log.Println("Unmount fail:", err)
	} else {
		// If the unmount fails, the RemoveAll will stat all
		// of the FUSE file system.
		os.RemoveAll(me.tmpDir)
	}
}

func (me *workerFuseFs) SetDebug(debug bool) {
	me.MountState.Debug = debug
	me.fsConnector.Debug = debug
	me.nodeFs.Debug = debug
	me.unionNodeFs.Debug = debug
}

func (me *Mirror) returnFuse(wfs *workerFuseFs) {
	me.fuseFileSystemsMutex.Lock()
	defer me.fuseFileSystemsMutex.Unlock()

	wfs.task = nil
	wfs.SetDebug(false)

	if me.shuttingDown {
		wfs.Stop()
	} else {
		me.unusedFileSystems = append(me.unusedFileSystems, wfs)
	}
	me.workingFileSystems[wfs] = "", false
	me.cond.Broadcast()
}

func newWorkerFuseFs(tmpDir string, rpcFs fuse.FileSystem, writableRoot string, nobody *user.User) (*workerFuseFs, os.Error) {
	tmpDir, err := ioutil.TempDir(tmpDir, "termite-task")
	if err != nil {
		return nil, err
	}
	me := &workerFuseFs{
		tmpDir: tmpDir,
		writableRoot: strings.TrimLeft(writableRoot, "/"),
	}

	type dirInit struct {
		dst *string
		val string
	}

	tmpBacking := ""
	for _, v := range []dirInit{
		dirInit{&me.rwDir, "rw"},
		dirInit{&me.mount, "mnt"},	
		dirInit{&tmpBacking, "tmp-backing"},
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
	
	me.nodeFs = fuse.NewPathNodeFs(rpcFs, nil)
	ttl := 30.0
	mOpts := fuse.FileSystemOptions{
		EntryTimeout:    ttl,
		AttrTimeout:     ttl,
		NegativeTimeout: ttl,

		// 32-bit programs have trouble with 64-bit inode
		// numbers.
		PortableInodes: true,
	}
		
	me.fsConnector = fuse.NewFileSystemConnector(me.nodeFs, &mOpts)
	me.MountState = fuse.NewMountState(me.fsConnector)
	err = me.MountState.Mount(me.mount, &fuseOpts)
	if err != nil {
		return nil, err
	}
	go me.MountState.Loop()
	
	rwFs := fuse.NewLoopbackFileSystem(me.rwDir)
	opts := unionfs.UnionFsOptions{
		BranchCacheTTLSecs:   ttl,
		DeletionCacheTTLSecs: ttl,
		DeletionDirName:      _DELETIONS,
	}
	me.unionFs = unionfs.NewUnionFs([]fuse.FileSystem{rwFs,
		&fuse.PrefixFileSystem{rpcFs, me.writableRoot}}, opts)

	me.procFs = NewProcFs()
	me.procFs.StripPrefix = me.mount
	if nobody != nil {
		me.procFs.Uid = nobody.Uid
	}
	type submount struct {
		mountpoint string
		fs         fuse.NodeFileSystem
	}
	
	mounts := []submount{
		{"proc", fuse.NewPathNodeFs(me.procFs, nil)},
		{"sys", fuse.NewPathNodeFs(&fuse.ReadonlyFileSystem{fuse.NewLoopbackFileSystem("/sys")}, nil)},
		{"tmp", fuse.NewMemNodeFs(tmpBacking + "tmp")},
		{"dev", NewDevNullFs()},
		{"var/tmp", fuse.NewMemNodeFs(tmpBacking + "vartmp")},
	}
	for _, s := range mounts {
		code := me.nodeFs.Mount(s.mountpoint, s.fs, nil)
		if !code.Ok() {
			me.MountState.Unmount()
			return nil, os.NewError(fmt.Sprintf("submount error for %s: %v", s.mountpoint, code))
		}
	}
	if strings.HasPrefix(me.writableRoot, "tmp/") {
		parent, _ := filepath.Split(me.writableRoot)
		err := os.MkdirAll(filepath.Join(me.mount, parent), 0755)
		if err != nil {
			me.MountState.Unmount()
			return nil, os.NewError(fmt.Sprintf("Mkdir of %q in /tmp fail: %v", parent, err))
		}
	}
	me.unionNodeFs = fuse.NewPathNodeFs(me.unionFs, &fuse.PathNodeFsOptions{ClientInodes: true})
	code := me.nodeFs.Mount(me.writableRoot, me.unionNodeFs, nil)
	if !code.Ok() {
		me.MountState.Unmount()
		return nil, os.NewError(fmt.Sprintf("submount error for %s: %v", me.writableRoot, code))
	}
	
	return me, nil
}

func (me *workerFuseFs) update(attrs []*FileAttr, origin *workerFuseFs) {
	paths := []string{}
	if me == origin {
		// TODO - should reread inode numbers, in case they
		// are reused.
	}

	for _, attr := range attrs {
		path := strings.TrimLeft(attr.Path, "/")
		if !strings.HasPrefix(path, me.writableRoot) {
			log.Printf("invalid prefix on %q, expect %q", path, me.writableRoot)
			continue
		}
		path = strings.TrimLeft(path[len(me.writableRoot):], "/")
		paths = append(paths, path)

		if origin == me {
			continue
		}
		
		if attr.Status.Ok() {
			log.Printf("ok notify %q", path)
			me.unionNodeFs.Notify(path)
		} else {
			// Even if GetAttr() returns ENOENT, FUSE will
			// happily try to Open() the file afterwards.
			// So, issue entry notify for deletions rather
			// than inode notify.
			dir, base := filepath.Split(path)
			dir = filepath.Clean(dir)
			me.unionNodeFs.EntryNotify(dir, base)
		}
	}
	me.unionFs.DropBranchCache(paths)
	me.unionFs.DropDeletionCache()
}
