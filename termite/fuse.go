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

type WorkerFuseFs struct {
	rwDir  string
	tmpDir string
	mount  string
	*fuse.MountState
	fsConnector *fuse.FileSystemConnector
	unionFs     *unionfs.UnionFs
	procFs      *ProcFs

	// If nil, we are running this task.
	task        *WorkerTask
}

func (me *WorkerFuseFs) Stop() {
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

func (me *WorkerFuseFs) SetDebug(debug bool) {
	me.fuseFs.MountState.Debug = debug
	me.fuseFs.fsConnector.Debug = debug
}

func (me *Mirror) returnFuse(wfs *WorkerFuseFs) {
	me.fuseFileSystemsMutex.Lock()
	defer me.fuseFileSystemsMutex.Unlock()

	wfs.task = nil
	wfs.procFs.SelfPid = 1
	wfs.SetDebug(false)
	
	if me.shuttingDown {
		wfs.Stop()
	} else {
		me.unusedFileSystems = append(me.unusedFileSystems, wfs)
	}
	me.workingFileSystems[wfs] = "", false
	me.cond.Broadcast()
}

func newWorkerFuseFs(tmpDir string, rpcFs fuse.FileSystem, writableRoot string,
	nobody *user.User) (*WorkerFuseFs, os.Error) {
	tmpDir, err := ioutil.TempDir(tmpDir, "termite-task")
	if err != nil {
		return nil, err
	}
	w := WorkerFuseFs{
		tmpDir: tmpDir,
	}

	type dirInit struct {
		dst *string
		val string
	}

	for _, v := range []dirInit{
		dirInit{&w.rwDir, "rw"},
		dirInit{&w.mount, "mnt"},
	} {
		*v.dst = filepath.Join(w.tmpDir, v.val)
		err = os.Mkdir(*v.dst, 0700)
		if err != nil {
			return nil, err
		}
	}

	tmpBacking := filepath.Join(w.tmpDir, "tmp-backingstore")
	if err := os.Mkdir(tmpBacking, 0700); err != nil {
		return nil, err
	}

	rwFs := fuse.NewLoopbackFileSystem(w.rwDir)

	ttl := 30.0
	opts := unionfs.UnionFsOptions{
		BranchCacheTTLSecs:   ttl,
		DeletionCacheTTLSecs: ttl,
		DeletionDirName:      _DELETIONS,
	}
	mOpts := fuse.FileSystemOptions{
		EntryTimeout:    ttl,
		AttrTimeout:     ttl,
		NegativeTimeout: ttl,
	}

	tmpFs := fuse.NewLoopbackFileSystem(tmpBacking)

	w.procFs = NewProcFs()
	w.procFs.StripPrefix = w.mount
	w.procFs.Uid = nobody.Uid
	w.procFs.AllowedRootFiles = map[string]int{
		"meminfo": 1,
		"cpuinfo": 1,
		"iomem": 1,
		"ioport": 1,
		"loadavg": 1,
		"stat": 1,
		"self": 1,
		"filesystems": 1,
		"mounts": 1,
	}

	w.unionFs = unionfs.NewUnionFs([]fuse.FileSystem{rwFs, rpcFs}, opts)
	swFs := []fuse.SwitchedFileSystem{
		{"", rpcFs, false},
		// TODO - configurable.
		{writableRoot, w.unionFs, false},
	}
	type submount struct {
		mountpoint string
		fs fuse.FileSystem
	}
	mounts := []submount{
		{"/proc", w.procFs},
		{"/sys", &fuse.ReadonlyFileSystem{fuse.NewLoopbackFileSystem("/sys")}},
		{"/dev", NewDevnullFs()},
	}
	fuseOpts := fuse.MountOptions{
		// Compilers are not that highly parallel.  A lower
		// number also helps stacktrace be less overwhelming.
		MaxBackground: 4,
	}
	if os.Geteuid() != 0 {
		// Typically, we run our tests as non-root under /tmp.
		// If we use go-fuse to mount /tmp, it will hide
		// writableRoot, and all our tests will fail.
		swFs = append(swFs,
			fuse.SwitchedFileSystem{"/tmp", tmpFs, true},
			fuse.SwitchedFileSystem{"var/tmp", tmpFs, true})
	} else {
		fuseOpts.AllowOther = true
		mounts = append(mounts,
			submount{"/tmp", tmpFs},
			submount{"/var/tmp", tmpFs})
	}
	
	w.fsConnector = fuse.NewFileSystemConnector(fuse.NewSwitchFileSystem(swFs), &mOpts)
	w.MountState = fuse.NewMountState(w.fsConnector)

	err = w.MountState.Mount(w.mount, &fuseOpts)
	if err != nil {
		return nil, err
	}
	for _, s := range mounts {
		code := w.fsConnector.Mount(s.mountpoint, s.fs, nil)
		if !code.Ok() {
			return nil, os.NewError(fmt.Sprintf("submount error for %v: %v", s.mountpoint, code))
		}
	}

	go w.MountState.Loop(true)

	return &w, nil
}

func (me *WorkerFuseFs) update(attrs []FileAttr) {
	paths := []string{}
	for _, attr := range attrs {
		path := strings.TrimLeft(attr.Path, "/")
		paths = append(paths, path)

		if attr.Status.Ok() {
			me.fsConnector.Notify(path)
		} else {
			// Even if GetAttr() returns ENOENT, FUSE will
			// happily try to Open() the file afterwards.
			// So, issue entry notify for deletions rather
			// than inode notify.
			dir, base := filepath.Split(path)
			dir = filepath.Clean(dir)
			me.fsConnector.EntryNotify(dir, base)
		}
	}
	me.unionFs.DropBranchCache(paths)
	me.unionFs.DropDeletionCache()
}
