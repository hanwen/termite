package termite

import (
	"github.com/hanwen/go-fuse/fuse"
	"github.com/hanwen/go-fuse/unionfs"
	"os"
	"path/filepath"
	"io/ioutil"
)

type WorkerFuseFs struct {
	rwDir  string
	tmpDir string
	mount  string
	*fuse.MountState
	unionFs *unionfs.UnionFs
}

func (me *WorkerFuseFs) Stop() {
	me.MountState.Unmount()
	os.RemoveAll(me.tmpDir)
}

func (me *Mirror) ReturnFuse(wfs *WorkerFuseFs) {
	// TODO - could be more fine-grained here.
	wfs.unionFs.DropBranchCache()
	wfs.unionFs.DropDeletionCache()

	me.fuseFileSystemsMutex.Lock()
	defer me.fuseFileSystemsMutex.Unlock()

	if !me.shuttingDown {
		wfs.Stop()
	} else {
		me.fuseFileSystems = append(me.fuseFileSystems, wfs)
	}
	me.workingFileSystems[wfs] = "", false
	me.cond.Signal()
}

func newWorkerFuseFs(workerDir string, rpcFs fuse.FileSystem, writableRoot string) (*WorkerFuseFs, os.Error) {
	tmpDir, err := ioutil.TempDir(
		filepath.Join(workerDir, "tmp"), "task")
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

	rwFs := fuse.NewLoopbackFileSystem(w.rwDir)

	ttl := 5.0
	opts := unionfs.UnionFsOptions{
		BranchCacheTTLSecs:   ttl,
		DeletionCacheTTLSecs: ttl,
		DeletionDirName:      _DELETIONS,
	}
	mOpts := fuse.FileSystemOptions{
		EntryTimeout:    ttl,
		AttrTimeout:     ttl,
		NegativeTimeout: 0.01,
	}

	w.unionFs = unionfs.NewUnionFs("ufs", []fuse.FileSystem{rwFs, rpcFs}, opts)
	swFs := []fuse.SwitchedFileSystem{
		{"dev", &DevNullFs{}, true},
		{"", rpcFs, false},

		// TODO - configurable.
		{"tmp", w.unionFs, false},
		{writableRoot, w.unionFs, false},
	}

	conn := fuse.NewFileSystemConnector(fuse.NewSwitchFileSystem(swFs), &mOpts)
	w.MountState = fuse.NewMountState(conn)

	fuseOpts := fuse.MountOptions{
		AllowOther: true,
		// Compilers are not that highly parallel.  A lower
		// number also helps stacktrace be less overwhelming.
		MaxBackground: 4,
	}
	w.MountState.Mount(w.mount, &fuseOpts)
	if err != nil {
		return nil, err
	}

	go w.MountState.Loop(true)

	return &w, nil
}
