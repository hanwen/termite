package termite

import (
	"github.com/hanwen/go-fuse/fuse"
	"github.com/hanwen/go-fuse/unionfs"
	"io/ioutil"
	"log"
	"os"
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

	if me.shuttingDown {
		wfs.Stop()
	} else {
		me.fuseFileSystems = append(me.fuseFileSystems, wfs)
	}
	me.workingFileSystems[wfs] = "", false
	me.cond.Signal()
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

	w.unionFs = unionfs.NewUnionFs([]fuse.FileSystem{rwFs, rpcFs}, opts)
	swFs := []fuse.SwitchedFileSystem{
		{"dev", &DevNullFs{}, true},
		{"", rpcFs, false},
		{"tmp", tmpFs, true},
		{"var/tmp", tmpFs, true},

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

		// We only invalidate files.  This is a kludge - we
		// should skip invalidations on dirs that we know that
		// they held files.
		if attr.FileInfo != nil && !attr.FileInfo.IsDirectory() {
			// TODO - should have bulk interface?
			dir, base := filepath.Split(path)
			dir = filepath.Clean(dir)
			
			me.fsConnector.EntryNotify(dir, base)
		}
	}
	me.unionFs.DropBranchCache(paths)
	me.unionFs.DropDeletionCache()
}
