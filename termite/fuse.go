package termite

import (
	"github.com/hanwen/go-fuse/fuse"
	"github.com/hanwen/go-fuse/unionfs"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"
	"os/user"
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

func (me *Mirror) returnFuse(wfs *WorkerFuseFs) {
	me.fuseFileSystemsMutex.Lock()
	defer me.fuseFileSystemsMutex.Unlock()

	wfs.task = nil
	wfs.procFs.SelfPid = 1
	if me.shuttingDown {
		wfs.Stop()
	} else {
		me.fuseFileSystems = append(me.fuseFileSystems, wfs)
	}
	me.workingFileSystems[wfs] = "", false
	me.cond.Broadcast()
}

func newWorkerFuseFs(tmpDir string, rpcFs fuse.FileSystem, writableRoot string) (*WorkerFuseFs, os.Error) {
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

	// TODO - pass in uid/gid from outside.
	nobody, err := user.Lookup("nobody")
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
	// TODO - use mounts for the strip versions of the filesystems.
	swFs := []fuse.SwitchedFileSystem{
		{"dev", NewDevnullFs(), true},
		{"", rpcFs, false},
		{"tmp", tmpFs, true},
		{"var/tmp", tmpFs, true},
		{"proc", w.procFs, true},
		{"sys", &fuse.ReadonlyFileSystem{fuse.NewLoopbackFileSystem("/sys")}, true},
		// TODO - configurable.
		{writableRoot, w.unionFs, false},
	}

	w.fsConnector = fuse.NewFileSystemConnector(fuse.NewSwitchFileSystem(swFs), &mOpts)
	w.MountState = fuse.NewMountState(w.fsConnector)

	fuseOpts := fuse.MountOptions{
		// Compilers are not that highly parallel.  A lower
		// number also helps stacktrace be less overwhelming.
		MaxBackground: 4,
	}
	if os.Geteuid() == 0 {
		fuseOpts.AllowOther = true
	}

	err = w.MountState.Mount(w.mount, &fuseOpts)
	if err != nil {
		return nil, err
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
